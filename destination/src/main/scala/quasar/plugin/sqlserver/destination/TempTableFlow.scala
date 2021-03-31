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

package quasar.plugin.sqlserver.destination

import slamdata.Predef._

import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.MonadResourceErr
import quasar.lib.jdbc.{Ident, Slf4sLogHandler}
import quasar.lib.jdbc.destination.WriteMode
import quasar.plugin.sqlserver._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.commit

import fs2.Chunk

import java.lang.CharSequence

import org.slf4s.Logger

sealed trait TempTableFlow {
  def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit]
  def replace(writeMode: WriteMode): ConnectionIO[Unit]
  def append: ConnectionIO[Unit]
}

object TempTableFlow {
  def apply[F[_]: Sync: MonadResourceErr](
      xa: Transactor[F],
      logger: Logger,
      path: ResourcePath,
      schema: String,
      columns: NonEmptyList[(HI, SQLServerType)],
      idColumn: Option[Column[_]])
      : Resource[F, TempTableFlow] = {

    val log = Slf4sLogHandler(logger)

    val acquire: F[(TempTable, TempTableFlow)] = for {
      (objFragment, unsafeName, unsafeSchema) <- pathFragment[F](schema, path)
      tempTable = TempTable(log, unsafeName, unsafeSchema, objFragment, columns, idColumn)
      _ <- {
        tempTable.drop >>
        tempTable.create >>
        commit
      }.transact(xa)
    } yield {
      val flow = new TempTableFlow {
        def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit] =
          tempTable.ingest(chunk) >> commit
        def replace(writeMode: WriteMode) =
          tempTable.persist(writeMode) >> commit
        def append =
          tempTable.append >> commit
      }
      (tempTable, flow)
    }
    val release: ((TempTable, TempTableFlow)) => F[Unit] = { case (tempTable, _) =>
      (tempTable.drop >> commit).transact(xa)
    }
    Resource.make(acquire)(release).map(_._2)
  }

  private trait TempTable {
    def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit]
    def drop: ConnectionIO[Unit]
    def create: ConnectionIO[Unit]
    def persist(writeMode: WriteMode): ConnectionIO[Unit]
    def append: ConnectionIO[Unit]
  }

  private object TempTable {
    def apply(
        log: LogHandler,
        unsafeName: String,
        unsafeSchema: Option[String],
        objFragment: Fragment,
        columns: NonEmptyList[(HI, SQLServerType)],
        idColumn: Option[Column[_]])
        : TempTable = {
      val schema = unsafeSchema.getOrElse("dbo")
      val tempName = s"precog_temp_$unsafeName"
      val hyName = SQLServerHygiene.hygienicIdent(Ident(tempName))
      val hySchema = SQLServerHygiene.hygienicIdent(Ident(schema))
      val obj = Fragment.const0(hySchema.forSqlName) ++ fr0"." ++ Fragment.const0(hyName.forSqlName)
      val uName = hyName.unsafeForSqlName
      val uSchema = hySchema.unsafeForSqlName

      new TempTable {
        def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit] =
          insertChunk(log)(obj, columns, chunk)

        def drop: ConnectionIO[Unit] =
          ifExists(log)(uName, uSchema.some).option flatMap { results =>
            if (results.exists(_ === 1)) {
              (fr"DROP TABLE" ++ obj)
                .updateWithLogHandler(log)
                .run
                .void
            } else {
              ().pure[ConnectionIO]
            }
          }

        def create: ConnectionIO[Unit] =
          ifExists(log)(uName, uSchema.some).option flatMap { results =>
            if (results.exists(_ === 0)) {
              val columnsObj = createColumnSpecs(columns)
              (fr"CREATE TABLE" ++ obj ++ fr0" " ++ columnsObj)
                .updateWithLogHandler(log)
                .run
                .void
            } else {
              ().pure[ConnectionIO]
            }
          }

        private def truncate: ConnectionIO[Unit] =
          ifExists(log)(uName, uSchema.some).option flatMap { results =>
            if (results.exists(_ === 1)) {
              (fr"TRUNCATE TABLE" ++ obj)
                .updateWithLogHandler(log)
                .run
                .void
            } else {
              ().pure[ConnectionIO]
            }
          }

        private def insertInto: ConnectionIO[Unit] =
          (fr"INSERT INTO" ++
            objFragment ++ fr0" " ++
            fr"SELECT * FROM" ++
            obj)
            .updateWithLogHandler(log)
            .run
            .void

        private def rename: ConnectionIO[Unit] =
          (fr0"EXEC SP_RENAME '" ++
            obj ++ fr0"', '" ++
            Fragment.const0(SQLServerHygiene.hygienicIdent(Ident(unsafeName)).unsafeForSqlName) ++ fr0"'")
            .updateWithLogHandler(log)
            .run
            .void

        private def filterTempIds(idColumn: Column[_]): ConnectionIO[Unit] = {
          val mkColumn: String => Fragment = parent =>
            Fragment.const0(parent) ++ fr0"." ++
            Fragment.const0(SQLServerHygiene.hygienicIdent(Ident(idColumn.name)).forSqlName)

          val fragment = fr"DELETE target FROM" ++ objFragment ++ fr" target INNER JOIN" ++ obj ++ fr" temp" ++
            fr"ON" ++ mkColumn("target") ++ fr0"=" ++ mkColumn("temp")

          fragment.updateWithLogHandler(log).run.void
        }

        def append: ConnectionIO[Unit] = {
          val mbFilter = idColumn traverse_ { col => filterTempIds(col) }
          mbFilter >>
          insertInto >>
          truncate >>
          commit
        }

        def persist(writeMode: WriteMode): ConnectionIO[Unit] = {
          val prepare = writeMode match {
            case WriteMode.Create =>
              createTable(log)(objFragment, columns) >>
              insertInto >>
              truncate
            case WriteMode.Replace =>
              dropTableIfExists(log)(objFragment) >>
              rename >>
              // We don't remove temp table in `persist`, it's handled in Resource instead.
              create
            case WriteMode.Truncate =>
              // This is `insertInto` instead of `renameTable` because user might want to preserve indices and so on
              truncateTable(log)(objFragment, unsafeName, unsafeSchema, columns)  >>
              insertInto >>
              truncate
            case WriteMode.Append =>
              createTableIfNotExists(log)(objFragment, unsafeName, unsafeSchema, columns) >>
              insertInto >>
              truncate
          }

          val mbCreateIndex = idColumn traverse_ { col =>
            val colFragment = Fragments.parentheses {
              Fragment.const(SQLServerHygiene.hygienicIdent(Ident(col.name)).forSqlName)
            }
            createIndex(log)(objFragment, unsafeName, colFragment)
          }
          prepare >> mbCreateIndex
        }
      }
    }
  }
}
