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
      t => Fragment.const0(t.forSql),
      { case (d, t) => Fragment.const0(d.forSql) ++ fr0"." ++ Fragment.const0(t.forSql) })

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

    // TODO SQLServerBulkCopy#close()
    // TODO date time things
    // TODO other csv escaping?
    // TODO SQLServerBulkCSVFileRecord.setEscapeColumnDelimitersCSV(boolean) ?
    def loadCsv(bytes: InputStream, connection: java.sql.Connection)
        : ConnectionIO[Unit] =
      for {
        bulkCopy <- FC.delay(new SQLServerBulkCopy(connection))
        bulkCSV <- FC.delay(new SQLServerBulkCSVFileRecord(bytes, "UTF-8", ",", true))
        bulkOptions <- FC.delay(new SQLServerBulkCopyOptions())

        _ <- FC.delay(bulkOptions.setBulkCopyTimeout(0)) // no timeout
        _ <- FC.delay(bulkOptions.setTableLock(true))
        _ <- FC.delay(bulkOptions.setUseInternalTransaction(false)) // transactions are managed externally
        _ <- FC.delay(bulkOptions.setBatchSize(512))

        _ <- FC.delay(bulkCopy.setDestinationTableName(objFragment.toString)) // TODO proper toString
        _ <- FC.delay(bulkCopy.setBulkCopyOptions(bulkOptions))
        //_ <- FC.delay(bulkCopy.addColumnMapping("", "")) // TODO add column mappings?
        //
        _ <- FC delay {
          cols.zipWithIndex.toList foreach {
            case ((name, tpe), idx) =>
              bulkCSV.addColumnMetadata(idx + 1, name.toString, doobie.enum.JdbcType.Double.toInt, 10, 10)
          }
        }

        _ <- FC.delay(bulkCopy.writeToServer(bulkCSV))
        _ <- FC.delay(bulkCopy.close())
      } yield ()

    def doLoad(bytes: InputStream, connection: java.sql.Connection)
        : ConnectionIO[Unit] =
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

        _ <- loadCsv(bytes, connection)
      } yield ()

    bytes => {
      Stream.resource(xa.connect(xa.kernel)) flatMap { connection =>
        val unwrapped = connection.unwrap(classOf[SQLServerConnection])
        bytes.through(fs2.io.toInputStream[F]).evalMap(doLoad(_, unwrapped).transact(xa))
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
