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

import quasar.connector.render.RenderConfig

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Chunk

package object destination {
  val NullSentinel = ""

  val SQLServerCsvConfig: RenderConfig.Csv =
    RenderConfig.Csv(
      includeHeader = false,
      nullSentinel = Some(NullSentinel),
      includeBom = false,
      booleanFormat = if (_) "1" else "0")

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
      : ConnectionIO[Int] = {
    val drop = (fr"DROP TABLE IF EXISTS" ++ objFragment)
      .updateWithLogHandler(logHandler)
      .run

    drop >> createTable(logHandler)(objFragment, columns)
  }

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
}
