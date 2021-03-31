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
import quasar.lib.jdbc.Ident
import quasar.lib.jdbc.destination.WriteMode
import quasar.plugin.sqlserver._

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._

final case class TempTable(obj: Fragment, unsafeName: String, unsafeSchema: String)

object TempTable {
  def fromName(unsafeName: String, optSchema: Option[String]): TempTable = {
    val schema = optSchema.getOrElse("dbo")
    val tempName = s"precog_temp_$unsafeName"
    val hySchema = SQLServerHygiene.hygienicIdent(Ident(schema))
    val hyName = SQLServerHygiene.hygienicIdent(Ident(tempName))
    val obj = Fragment.const0(hySchema.forSqlName) ++ fr0"." ++ Fragment.const0(hyName.forSqlName)
    TempTable(obj, hyName.unsafeForSqlName, hySchema.unsafeForSqlName)
  }

  def dropTempTable(log: LogHandler)(tempTable: TempTable): ConnectionIO[Unit] =
    ifExists(log)(tempTable.unsafeName, tempTable.unsafeSchema.some).option flatMap { results =>
      if (results.exists(_ === 1)) {
        (fr"DROP TABLE" ++ tempTable.obj)
          .updateWithLogHandler(log)
          .run
          .void
      } else {
        ().pure[ConnectionIO]
      }
    }

  def createTempTable(log: LogHandler)(tempTable: TempTable, columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Unit] =
    ifExists(log)(tempTable.unsafeName, tempTable.unsafeSchema.some).option flatMap { results =>
      if (results.exists(_ === 0)) {
        val columnsObj = createColumnSpecs(columns)
        (fr"CREATE TABLE" ++ tempTable.obj ++ fr0" " ++ columnsObj)
          .updateWithLogHandler(log)
          .run
          .void
      } else {
        ().pure[ConnectionIO]
      }
    }

  def truncateTempTable(log: LogHandler)(tempTable: TempTable): ConnectionIO[Unit] =
    ifExists(log)(tempTable.unsafeName, tempTable.unsafeSchema.some).option flatMap { results =>
      if (results.exists(_ === 1)) {
        (fr"TRUNCATE TABLE" ++ tempTable.obj)
          .updateWithLogHandler(log)
          .run
          .void
      } else {
        ().pure[ConnectionIO]
      }
    }

  def insertInto(log: LogHandler)(from: TempTable, target: Fragment): ConnectionIO[Unit] =
    (fr"INSERT INTO" ++
      target ++ fr0" " ++
      fr"SELECT * FROM" ++
      from.obj)
      .updateWithLogHandler(log)
      .run
      .void

  def renameTable(log: LogHandler)(from: TempTable, target: String): ConnectionIO[Unit] =
    (fr0"EXEC SP_RENAME '" ++
      from.obj ++ fr0"', '" ++
      Fragment.const0(SQLServerHygiene.hygienicIdent(Ident(target)).unsafeForSqlName) ++ fr0"'")
      .updateWithLogHandler(log)
      .run
      .void

  def applyTempTable(log: LogHandler)(
    writeMode: WriteMode,
    tempTable: TempTable,
    obj: Fragment,
    unsafeName: String,
    unsafeSchema: Option[String],
    columns: NonEmptyList[(HI, SQLServerType)],
    idColumn: Option[Column[_]])
      : ConnectionIO[Unit] = {
    val prepare = writeMode match {
      case WriteMode.Create =>
        createTable(log)(obj, columns) >>
        insertInto(log)(tempTable, obj) >>
        truncateTempTable(log)(tempTable)
      case WriteMode.Replace =>
        dropTableIfExists(log)(obj) >>
        renameTable(log)(tempTable, unsafeName) >>
        createTempTable(log)(tempTable, columns)
      case WriteMode.Truncate =>
        // This is `insertInto` instead of `renameTable` because user might want to preser indices and so on
        truncateTable(log)(obj, unsafeName, unsafeSchema, columns)  >>
        insertInto(log)(tempTable, obj) >>
        truncateTempTable(log)(tempTable)
      case WriteMode.Append =>
        createTableIfNotExists(log)(obj, unsafeName, unsafeSchema, columns) >>
        insertInto(log)(tempTable, obj) >>
        truncateTempTable(log)(tempTable)
    }

    val mbCreateIndex = idColumn traverse_ { col =>
      val colFragment = Fragments.parentheses(Fragment.const(SQLServerHygiene.hygienicIdent(Ident(col.name)).forSqlName))
      createIndex(log)(obj, unsafeName, colFragment)
    }

    prepare >> mbCreateIndex
  }

  def filterTempIds(log: LogHandler)(temp: TempTable, target: Fragment, idColumn: Column[_]): ConnectionIO[Unit] = {
    val mkColumn: String => Fragment = parent =>
      Fragment.const0(parent) ++ fr0"." ++
      Fragment.const0(SQLServerHygiene.hygienicIdent(Ident(idColumn.name)).forSqlName)

    val fragment = fr"DELETE target FROM" ++ target ++ fr" target INNER JOIN" ++ temp.obj ++ fr" temp" ++
      fr"ON" ++ mkColumn("target") ++ fr0"=" ++ mkColumn("temp")

    fragment.updateWithLogHandler(log).run.void
  }

}
