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

import quasar.connector.render.RenderConfig

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._

package object destination {
  val NullSentinel = ""

  val SQLServerCsvConfig: RenderConfig.Csv =
    RenderConfig.Csv(
      includeHeader = false,
      nullSentinel = Some(NullSentinel),
      includeBom = false,
      booleanFormat = if (_) "1" else "0")

  def ifExists(logHandler: LogHandler)(unsafeName: String): Query0[Int] = {
    (fr0"SELECT count(*) as exists_flag FROM [INFORMATION_SCHEMA].[TABLES] WHERE TABLE_NAME='" ++ Fragment.const0(unsafeName) ++ fr0"'")
      .queryWithLogHandler[Int](logHandler)
  }

  def replaceTable(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeName: String,
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
      if (result.exists(_ == 1)) {
        val drop = (fr"DROP TABLE" ++ objFragment)
          .updateWithLogHandler(logHandler)
          .run
        drop >> createTable(logHandler)(objFragment, columns)
      } else {
        createTable(logHandler)(objFragment, columns)
      }
    }

  def truncateTable(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeName: String,
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
      if (result.exists(_ == 1))
        (fr"TRUNCATE TABLE" ++ objFragment)
          .updateWithLogHandler(logHandler)
          .run
      else
        createTable(logHandler)(objFragment, columns)
    }

  def appendToTable(logHandler: LogHandler)(
      objFragment: Fragment,
      unsafeName: String,
      columns: NonEmptyList[(HI, SQLServerType)])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
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
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))

}
