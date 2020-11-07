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
  SQLServerBulkCSVFileRecord
}

import doobie._
import doobie.implicits._

import fs2.Pipe

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

    def createTable(ifNotExists: Boolean) = {
      val stmt = if (ifNotExists) fr"CREATE TABLE IF NOT EXISTS" else fr"CREATE TABLE"

      (stmt ++ objFragment ++ fr0" " ++ columnSpecs(cols))
        .updateWithLogHandler(logHandler)
        .run
    }

    def loadCsv(bytes: InputStream): ConnectionIO[Unit] = {
      val res: cats.effect.Resource[F, Unit] =
        xa.connect(xa.kernel) map { connection =>
          val bulkCopy = new SQLServerBulkCopy(connection)
          val bulkCSV = new SQLServerBulkCSVFileRecord(bytes, "UTF-8", ",", true)
          val bulkOptions = new SQLServerBulkCopyOptions()

          bulkOptions.setBulkCopyTimeout(0) // no timeout
          bulkOptions.setTableLock(true)
          bulkOptions.setUseInternalTransaction(false)
          bulkOptions.setBatchSize(128)

          bulkCopy.setDestinationTableName(objFragment.toString) // TODO proper toString
          bulkCopy.setBulkCopyOptions(bulkOptions)
          //bulkCopy.addColumnMapping("", "") // TODO add column mappings?
          bulkCopy.writeToServer(bulkCSV)
        }

      ???
    }

    def doLoad(bytes: InputStream) = for {
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

      _ <- loadCsv(bytes)
    } yield ()

    _.through(fs2.io.toInputStream[F]).evalMap(doLoad(_).transact(xa))
  }

  ////

  private def columnSpecs(cols: NonEmptyList[(HI, SQLServerType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))
}
