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
import scala.annotation.switch
import java.sql.ResultSet
import java.time._

import doobie.enum.JdbcType

import quasar.common.data.RValue
import quasar.plugin.jdbc.VendorType
import quasar.plugin.jdbc.datasource.{unsupportedColumnTypeMsg, ColumnNum, RValueColumn, SqlType}

object SQLServerRValueColumn extends RValueColumn {
  import java.sql.Types._
  import microsoft.sql.Types.DATETIMEOFFSET

  def isSupported(sqlType: SqlType, sqlServerType: VendorType): Boolean =
    SupportedSqlTypes(sqlType)

  // TODO make sure this aligns with isSupported
  def unsafeRValue(rs: ResultSet, col: ColumnNum, sqlType: SqlType, vendorType: VendorType): RValue = {
    @inline
    def unlessNull[A](a: A)(f: A => RValue): RValue =
      if (a == null) null else f(a)

    def unsupported =
      RValue.rString(unsupportedColumnTypeMsg(JdbcType.fromInt(sqlType), vendorType))

    (sqlType: @switch) match {
      case CHAR | NCHAR | VARCHAR | NVARCHAR | LONGVARCHAR =>
        unlessNull(rs.getString(col))(RValue.rString(_))

      case TINYINT | SMALLINT | INTEGER | BIGINT =>
        unlessNull(rs.getLong(col))(RValue.rLong(_))

      case DOUBLE | REAL =>
        unlessNull(rs.getDouble(col))(RValue.rDouble(_))

      case DECIMAL | NUMERIC =>
        unlessNull(rs.getBigDecimal(col))(RValue.rNum(_))

      case BIT =>
        unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))

      case DATE =>
        unlessNull(rs.getObject(col, classOf[LocalDate]))(RValue.rLocalDate(_))

      case TIME =>
        unlessNull(rs.getObject(col, classOf[LocalTime]))(RValue.rLocalTime(_))

      case TIMESTAMP =>
        unlessNull(rs.getObject(col, classOf[LocalDateTime]))(RValue.rLocalDateTime(_))

      case DATETIMEOFFSET =>
        unlessNull(rs.getObject(col, classOf[OffsetDateTime]))(RValue.rOffsetDateTime(_))

      case _ => unsupported
    }
  }

  ////

  private val SupportedSqlTypes: Set[Int] =
    Mapping.JdbcColumnTypes.keySet.map(_.toInt)
}
