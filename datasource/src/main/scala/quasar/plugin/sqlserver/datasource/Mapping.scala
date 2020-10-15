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

package quasar.plugin.sqlserver.datasource

import scala._, Predef._

import doobie.enum.JdbcType

import quasar.api.ColumnType
import quasar.plugin.jdbc._

object Mapping {
  import JdbcType._

  val YEAR = "year"
  val TINYINT = "tinyint"

  val JdbcColumnTypes: Map[JdbcType, ColumnType.Scalar] =
    Map(
      BigInt -> ColumnType.Number,
      Char -> ColumnType.String,
      Date -> ColumnType.LocalDate,
      Decimal -> ColumnType.Number,
      Double -> ColumnType.Number,
      Integer -> ColumnType.Number,
      LongVarChar -> ColumnType.String,
      Real -> ColumnType.Number,
      SmallInt -> ColumnType.Number,
      Time -> ColumnType.LocalTime,
      Timestamp -> ColumnType.LocalDateTime,
      TinyInt -> ColumnType.Number,
      VarChar -> ColumnType.String)

  val SQLServerColumnTypes: Map[VendorType, ColumnType.Scalar] =
    Map()
}
