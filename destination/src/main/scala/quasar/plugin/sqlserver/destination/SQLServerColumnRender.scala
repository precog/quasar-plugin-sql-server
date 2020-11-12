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

import scala._, Predef._

import java.lang.CharSequence
import java.time._
import java.time.format.DateTimeFormatter

import quasar.api.Column
import quasar.connector.render.ColumnRender

import cats.data.NonEmptyList

import qdata.time.{DateTimeInterval, OffsetDate}

final class SQLServerColumnRender private (columns: Map[String, SQLServerType])
    extends ColumnRender[CharSequence] {

  def renderUndefined(columnName: String): CharSequence = "NULL"

  def renderNull(columnName: String): CharSequence = "NULL"

  def renderEmptyArray(columnName: String): CharSequence = renderUndefined(columnName)

  def renderEmptyObject(columnName: String): CharSequence = renderUndefined(columnName)

  def renderBoolean(columnName: String, value: Boolean): CharSequence = if (value) "1" else "0"

  def renderLong(columnName: String, value: Long): CharSequence = value.toString

  def renderDouble(columnName: String, value: Double): CharSequence = value.toString

  def renderBigDecimal(columnName: String, value: BigDecimal): CharSequence = value.toString

  // TODO precend with N'sdfsdfs'
  def renderString(columnName: String, value: String): CharSequence = quote(value.replaceAll("'", "''"))

  def renderLocalTime(columnName: String, value: LocalTime): CharSequence = quote(value.format(DateTimeFormatter.ofPattern("HH:mm:ss")))

  def renderOffsetTime(columnName: String, value: OffsetTime): CharSequence = renderUndefined(columnName)

  def renderLocalDate(columnName: String, value: LocalDate): CharSequence = quote(value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

  def renderOffsetDate(columnName: String, value: OffsetDate): CharSequence = renderUndefined(columnName)

  def renderLocalDateTime(columnName: String, value: LocalDateTime): CharSequence =
    quote(value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS")))

  def renderOffsetDateTime(columnName: String, value: OffsetDateTime): CharSequence = renderUndefined(columnName)

  def renderInterval(columnName: String, value: DateTimeInterval): CharSequence = renderUndefined(columnName)

  ////

  private def quote(chars: CharSequence): CharSequence = s"'$chars'"
}

object SQLServerColumnRender {
  def apply(columns: NonEmptyList[Column[SQLServerType]]): ColumnRender[CharSequence] = {
    new SQLServerColumnRender(columns.map(col => (col.name, col.tpe)).toList.toMap)
  }
}
