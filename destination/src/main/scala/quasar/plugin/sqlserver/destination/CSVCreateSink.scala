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

import quasar.plugin.sqlserver._

import scala._, Predef._

import java.io.InputStream

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import com.microsoft.sqlserver.jdbc.{
  SQLServerBulkCopy,
  SQLServerBulkCopyOptions,
  SQLServerBulkCSVFileRecord,
  SQLServerConnection
}

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

import quasar.plugin.jdbc.Slf4sLogHandler
import quasar.plugin.jdbc.destination.WriteMode

private[destination] object CsvCreateSink {
  def apply[F[_]: ConcurrentEffect](
      writeMode: WriteMode,
      schema: HI,
      xa: Transactor[F],
      logger: Logger)(
      obj: Either[HI, (HI, HI)],
      cols: NonEmptyList[(HI, SQLServerType)])
      : Pipe[F, Byte, Unit] = {

    val logHandler = Slf4sLogHandler(logger)

    val objFragment = obj.fold(
      t => Fragment.const0(schema.forSql) ++ fr0"." ++ Fragment.const0(t.forSql),
      { case (d, t) => Fragment.const0(d.forSql) ++ fr0"." ++ Fragment.const0(t.forSql) })

    val unsafeObj = obj.fold(
      t => schema.unsafeString ++ "." ++ t.unsafeString,
      { case (d, t) => d.unsafeString ++ "." ++ t.unsafeString })

    def dropTableIfExists =
      (fr"DROP TABLE IF EXISTS" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    // TODO test table truncate
    def truncateTable =
      (fr"TRUNCATE" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    // TODO test table creation if not exists
    def createTable(ifNotExists: Boolean): ConnectionIO[Int] = {
      val stmt = if (ifNotExists) fr"CREATE TABLE IF NOT EXISTS" else fr"CREATE TABLE"

      //(stmt ++ objFragment ++ fr0" " ++ columnSpecs(cols))
      (stmt ++ Fragment.const0("[nope]") ++ fr0" " ++ columnSpecs(cols))
        .updateWithLogHandler(logHandler)
        .run
    }

    // TODO date time things
    // TODO other csv escaping?
    // TODO set SQLServerBulkCopyOptions ?
    def loadCsv(bytes: InputStream, connection: java.sql.Connection): F[Unit] = {
      type Utilities = (SQLServerBulkCopy, SQLServerBulkCSVFileRecord)

      val acquire: F[Utilities] = ConcurrentEffect[F] delay {
        val bulkCopy = new SQLServerBulkCopy(connection)
        val bulkCSV = new SQLServerBulkCSVFileRecord(bytes, "UTF-8", ",", false)

        (bulkCopy, bulkCSV)
      }

      val use: Utilities => F[Unit] = {
        case (bulkCopy, bulkCSV) => ConcurrentEffect[F] delay {
          cols.zipWithIndex.toList foreach {
            case ((_, tpe), idx) => // TODO use tpe
              bulkCSV.addColumnMetadata(idx + 1, "", java.sql.Types.INTEGER, 0, 0)
          }
          logger.debug(s"Added column metadata for $unsafeObj")

          bulkCopy.setDestinationTableName(unsafeObj)
          logger.debug(s"Set destination table name to $unsafeObj")

          bulkCopy.writeToServer(bulkCSV)
          logger.debug(s"Wrote bulk CSV to $unsafeObj")
        }
      }

      val release: Utilities => F[Unit] = {
        case (bulkCopy, _) => ConcurrentEffect[F] delay {
          bulkCopy.close()
        }
      }

      ConcurrentEffect[F].bracket(acquire)(use)(release)
    }

      //  try {
      //    cols.zipWithIndex.toList foreach {
      //      case ((_, tpe), idx) => // TODO use tpe
      //        bulkCSV.addColumnMetadata(idx + 1, "", java.sql.Types.INTEGER, 0, 0)
      //    }
      //    logger.debug(s"Added column metadata for $unsafeObj")

      //    bulkCopy.setDestinationTableName(unsafeObj)
      //    logger.debug(s"Set destination table name to $unsafeObj")

      //    bulkCopy.writeToServer(bulkCSV)
      //    logger.debug(s"Wrote bulk CSV to $unsafeObj")

      //    bulkCopy.close()
      //    logger.debug(s"Closed bulk CSV copy for $unsafeObj")
      //  } catch {
      //    // TODO catch this exception within the push so that the push errors
      //    case (e: Exception) => logger.warn(s"got exception: ${e.getMessage}")
      //  }
      //}

    def prepareTable: ConnectionIO[Unit] =
      for {
        _ <- writeMode match {
          case WriteMode.Create =>
            createTable(ifNotExists = false)

          case WriteMode.Replace =>
            dropTableIfExists >> createTable(ifNotExists = false)

          case WriteMode.Truncate =>
            createTable(ifNotExists = true) >> truncateTable

          case WriteMode.Append =>
            createTable(ifNotExists = true)
        }
      } yield ()

    bytes => {
      Stream.resource(xa.connect(xa.kernel)) flatMap { connection =>
        val unwrapped = connection.unwrap(classOf[SQLServerConnection])

        Stream.eval(prepareTable.transact(xa)) ++
          bytes.through(fs2.io.toInputStream[F]).evalMap(loadCsv(_, unwrapped))
      }
    }
  }

  ////

  private def columnSpecs(cols: NonEmptyList[(HI, SQLServerType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))
}
