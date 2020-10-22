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

  def isSupported(sqlType: SqlType, sqlServerType: VendorType): Boolean =
    SupportedSqlTypes(sqlType) ||
    Mapping.SQLServerColumnTypes.contains(sqlServerType) ||
    (sqlType == BIT && sqlServerType == Mapping.TINYINT)

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

      case DOUBLE | FLOAT | REAL =>
        unlessNull(rs.getDouble(col))(RValue.rDouble(_))

      case DECIMAL | NUMERIC =>
        unlessNull(rs.getBigDecimal(col))(RValue.rNum(_))

      case BOOLEAN =>
        unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))

      case BIT =>
        if (vendorType == Mapping.TINYINT) // TODO is this correct?
          unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))
        else
          unsupported

      case DATE =>
        if (vendorType == Mapping.YEAR) // TODO is this correct?
          unlessNull(rs.getLong(col))(RValue.rLong(_))
        else
          unlessNull(rs.getObject(col, classOf[LocalDate]))(RValue.rLocalDate(_))

      case TIME =>
        unlessNull(rs.getObject(col, classOf[LocalTime]))(RValue.rLocalTime(_))

      case TIMESTAMP =>
        unlessNull(rs.getObject(col, classOf[LocalDateTime]))(RValue.rLocalDateTime(_))

      case _ => unsupported
    }
  }

  ////

  private val SupportedSqlTypes = Mapping.JdbcColumnTypes.keySet.map(_.toInt)
}
