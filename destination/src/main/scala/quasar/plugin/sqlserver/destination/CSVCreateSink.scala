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
      xa: Transactor[F],
      logger: Logger)(
      obj: Either[HI, (HI, HI)],
      cols: NonEmptyList[(HI, SQLServerType)])
      : Pipe[F, Byte, Unit] = {

    val logHandler = Slf4sLogHandler(logger)

    val objFragment = obj.fold(
      t => fr0"[dbo]." ++ Fragment.const0(t.forSql), // [dbo] is the default schema
      { case (d, t) => Fragment.const0(d.forSql) ++ fr0"." ++ Fragment.const0(t.forSql) })

    val unsafeObjNoHygiene = obj.fold(
      t => t.forSql.drop(1).dropRight(1),
      { case (d, t) => d.forSql.drop(1).dropRight(1) ++ "." ++ t.forSql.drop(1).dropRight(1) })

    def dropTableIfExists =
      (fr"DROP TABLE IF EXISTS" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    def truncateTable =
      (fr"TRUNCATE" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    def createTable(ifNotExists: Boolean): ConnectionIO[Int] = {
      val stmt = if (ifNotExists) fr"CREATE TABLE IF NOT EXISTS" else fr"CREATE TABLE"

      (stmt ++ objFragment ++ fr0" " ++ columnSpecs(cols))
        .updateWithLogHandler(logHandler)
        .run
    }

    // TODO date time things
    // TODO other csv escaping?
    def loadCsv(bytes: InputStream, connection: java.sql.Connection): F[Unit] =
      ConcurrentEffect[F].delay {
        val bulkCopy = new SQLServerBulkCopy(connection)
        val bulkCSV = new SQLServerBulkCSVFileRecord(bytes, "UTF-8", ",", false)

        try {

          cols.zipWithIndex.toList foreach {
            case ((_, tpe), idx) => // TODO use tpe
              bulkCSV.addColumnMetadata(idx + 1, "", java.sql.Types.INTEGER, 0, 0)
          }

          bulkCopy.setDestinationTableName(unsafeObjNoHygiene)

          bulkCopy.writeToServer(bulkCSV)
          logger.debug(s"Wrote bulk CSV to server.")

          bulkCopy.close()
          logger.debug(s"Closed bulk copy.")
        } catch {
          case (e: Exception) => logger.warn(s"got exception: ${e.getMessage}")
        }
      }

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
