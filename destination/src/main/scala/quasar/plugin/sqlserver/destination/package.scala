/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.sqlserver

import scala._, Predef._
import java.lang.CharSequence

import quasar.api.Column
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc
import quasar.lib.jdbc.Ident
import quasar.lib.jdbc.destination.WriteMode

import cats.{Applicative, Functor}
import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Chunk

package object destination {
  val NullSentinel = ""

  def renderConfig(cols: NonEmptyList[Column[SQLServerType]]): RenderConfig[CharSequence] =
    RenderConfig.Separated(",", SQLServerColumnRender(cols))

  def ifExists(logHandler: LogHandler)(
      unsafeTable: String,
      unsafeSchema: Option[String]): Query0[Int] = {
    val schemaFragment = unsafeSchema
      .map(s => fr0"' AND TABLE_SCHEMA='" ++ Fragment.const0(s))
      .getOrElse(fr0"")

    (fr0"SELECT count(*) as exists_flag FROM [INFORMATION_SCHEMA].[TABLES] WHERE TABLE_NAME='" ++
        Fragment.const0(unsafeTable) ++
        schemaFragment ++
        fr0"'")
      .queryWithLogHandler[Int](logHandler)
  }

  def replaceTable(logHandler: LogHandler)(
      objFragment: Fragment,
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    dropTableIfExists(logHandler)(objFragment) >> createTable(logHandler)(objFragment, columns)

  def dropTableIfExists(logHandler: LogHandler)(
      objFragment: Fragment)
      : ConnectionIO[Unit] =
    (fr"DROP TABLE IF EXISTS" ++ objFragment)
      .updateWithLogHandler(logHandler)
      .run
      .void


  def truncateTable(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeTable: String,
      unsafeSchema: Option[String],
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeTable, unsafeSchema).option flatMap { result =>
      if (result.exists(_ == 1))
        (fr"TRUNCATE TABLE" ++ objFragment)
          .updateWithLogHandler(logHandler)
          .run
      else
        createTable(logHandler)(objFragment, columns)
    }

  def appendToTable(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeTable: String,
      unsafeSchema: Option[String],
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeTable, unsafeSchema).option flatMap { result =>
      if (result.exists(_ == 1))
        0.pure[ConnectionIO]
      else
        createTable(logHandler)(objFragment, columns)
    }

  def createTableIfNotExists(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeTable: String,
      unsafeSchema: Option[String],
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeTable, unsafeSchema).option flatMap { result =>
      if (result.exists(_ == 0)) {
        createTable(logHandler)(objFragment, columns)
      } else {
        0.pure[ConnectionIO]
      }
    }
  def createTable(logHandler: LogHandler)(
      objFragment: Fragment,
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    (fr"CREATE TABLE" ++ objFragment ++ fr0" " ++ createColumnSpecs(columns))
      .updateWithLogHandler(logHandler)
      .run

  def createColumnSpecs(cols: NonEmptyList[(HI, SQLServerType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSqlName) ++ t.asSql }
        .intercalate(fr","))

  def insertColumnSpecs(cols: NonEmptyList[(HI, SQLServerType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, _) => Fragment.const(n.forSqlName) }
        .intercalate(fr","))

  def insertIntoPrefix(
    logHandler: LogHandler)(
    objFragment: Fragment,
    cols: NonEmptyList[(HI, SQLServerType)])
      : (StringBuilder, Int) = {
    val value = (
      fr"INSERT INTO" ++
        objFragment ++
        insertColumnSpecs(cols) ++
        fr0" VALUES (").updateWithLogHandler(logHandler).sql

    val builder = new StringBuilder(value)

    (builder, builder.length)
  }

  def insertChunk(
    logHandler: LogHandler)(
    objFragment: Fragment,
    cols: NonEmptyList[(HI, SQLServerType)],
    chunk: Chunk[CharSequence])
      : ConnectionIO[Unit] = {
    val (prefix, length) = insertIntoPrefix(logHandler)(objFragment, cols)
    val batch = FS.raw { statement =>
      chunk foreach { value =>
        val sql = prefix.append(value).append(')')

        statement.addBatch(sql.toString)
        prefix.setLength(length)
      }
      statement.executeBatch()
    }

    HC.createStatement(batch).void
  }

  def pathFragment[F[_]: Applicative: MonadResourceErr](scheme: String, path: ResourcePath)
      : F[(Fragment, String, Option[String])] = {
    val actualPath = path./:(ResourceName(scheme))
    val objF = jdbc.resourcePathRef(actualPath) match {
      case None => MonadResourceErr[F].raiseError(ResourceError.notAResource(actualPath))
      case Some(obj) => obj.pure[F]
    }
    objF map {
      case Left(t) =>
        val hy = SQLServerHygiene.hygienicIdent(t)
        (Fragment.const0(hy.forSqlName), hy.unsafeForSqlName, None)
      case Right((d, t)) =>
        val hyD = SQLServerHygiene.hygienicIdent(d)
        val hyT = SQLServerHygiene.hygienicIdent(t)
        (Fragment.const0(hyD.forSqlName) ++ fr0"." ++ Fragment.const0(hyT.forSqlName),
          hyT.unsafeForSqlName,
          hyD.unsafeForSqlName.some)
    }
  }

  def startLoad(logHandler: LogHandler)(
    writeMode: WriteMode,
    obj: Fragment,
    unsafeName: String,
    unsafeSchema: Option[String],
    columns: NonEmptyList[(HI, SQLServerType)],
    idColumn: Option[Column[_]])
      : ConnectionIO[Unit] = {
    val prepareTable = writeMode match {
      case WriteMode.Create => createTable(logHandler)(obj, columns)
      case WriteMode.Replace => replaceTable(logHandler)(obj, columns)
      case WriteMode.Truncate => truncateTable(logHandler)(obj, unsafeName, unsafeSchema, columns)
      case WriteMode.Append => appendToTable(logHandler)(obj, unsafeName, unsafeSchema, columns)
    }
    val mbCreateIndex = idColumn traverse_ { col =>
      val colFragment = Fragments.parentheses(Fragment.const(SQLServerHygiene.hygienicIdent(Ident(col.name)).forSqlName))
      createIndex(logHandler)(obj, unsafeName, colFragment)
    }
    prepareTable >> mbCreateIndex
  }

  def hygienicColumns[F[_]: Functor, A](cols: F[Column[A]]): F[(HI, A)] = cols map { c =>
    (SQLServerHygiene.hygienicIdent(Ident(c.name)), c.tpe)
  }

  def indexName(unsafeName: String): String =
    s"precog_id_idx_$unsafeName"

  def createIndex(log: LogHandler)(obj: Fragment, unsafeName: String, column: Fragment): ConnectionIO[Int] = {
    val strName = indexName(unsafeName)
    val idxName = Fragment.const(SQLServerHygiene.hygienicIdent(Ident(strName)).forSqlName)
    val doCreate = fr"CREATE INDEX" ++ idxName ++ fr"ON" ++ obj ++ column
    val fragment = fr"IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = '" ++
      Fragment.const0(strName) ++ fr0"' AND object_id = OBJECT_ID('" ++ Fragment.const0(unsafeName) ++
      fr"'))" ++ doCreate

    fragment.updateWithLogHandler(log).run
  }
}
