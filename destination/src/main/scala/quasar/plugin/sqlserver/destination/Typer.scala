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

import quasar.api.{ColumnType, Labeled}
import quasar.api.push.TypeCoercion
import quasar.api.push.param._
import quasar.connector.destination.Constructor

import scala._, Predef._

import cats.data.NonEmptyList
import cats.syntax.all._

object Typer {
  import Constructor._
  import SQLServerType._

  def coerce(scalarType: ColumnType.Scalar): TypeCoercion[SQLServerTypeId] =
    coercions(scalarType)

  def construct(id: SQLServerTypeId): Either[SQLServerType, Constructor[SQLServerType]] =
    id match {
      case tpe: SQLServerTypeId.SelfIdentified => Left(tpe)
      case hk: SQLServerTypeId.HigherKinded => Right(hk.constructor)
    }

  def inferScalar(tpe: SQLServerType): Option[ColumnType.Scalar] =
    coercions.toList
      .collect {
        case (k, TypeCoercion.Satisfied(ids)) if ids.exists(_ === tpe.id) => k
      } match {
        case t :: Nil => Some(t)
        case _ => None
      }

  /** Returns the "preferred" (highest priority) representation for a scalar
    * using maximal values for any parameters, when applicable.
    */
  def preferred(scalarType: ColumnType.Scalar): Option[SQLServerType] =
    coerce(scalarType) match {
      case TypeCoercion.Satisfied(ids) =>
        construct(ids.head) match {
          case Left(t) => Some(t)
          case Right(Unary(Labeled(_, p1), f)) =>
            Some(f(maximal(p1)))
          case Right(Binary(Labeled(_, p1), Labeled(_, p2), f)) =>
            Some(f(maximal(p1), maximal(p2)))
          case Right(Ternary(Labeled(_, p1), Labeled(_, p2), Labeled(_, p3), f)) =>
            Some(f(maximal(p1), maximal(p2), maximal(p3)))
        }

      case TypeCoercion.Unsatisfied(_, _) => None
    }

  ////

  private val coercions: Map[ColumnType.Scalar, TypeCoercion[SQLServerTypeId]] =
    Map(
      ColumnType.Boolean -> satisfied(BIT),

      ColumnType.LocalTime -> satisfied(TIME),
      ColumnType.LocalDate -> satisfied(DATE),
      ColumnType.LocalDateTime -> satisfied(DATETIME2, DATETIME, SMALLDATETIME),
      ColumnType.OffsetDateTime -> satisfied(DATETIMEOFFSET),

      ColumnType.OffsetTime ->
        TypeCoercion.Unsatisfied(List(ColumnType.LocalTime), None),

      ColumnType.OffsetDate ->
        TypeCoercion.Unsatisfied(List(ColumnType.LocalDate), None),

      ColumnType.Number ->
        satisfied(
          FLOAT,
          INT,
          DECIMAL,
          BIGINT,
          NUMERIC,
          REAL,
          SMALLINT,
          TINYINT),

      ColumnType.String ->
        satisfied(
          VARCHAR,
          TEXT,
          NTEXT,
          NCHAR,
          NVARCHAR,
          CHAR)
    ).withDefaultValue(TypeCoercion.Unsatisfied(Nil, None))

  private def maximal[A](formal: Formal[A]): Option[A] = {
    import ParamType._

    formal match {
      case Boolean(_) => Some(true)
      case Integer(Integer.Args(bounds, step, defaultValue)) =>
        Some((defaultValue orElse bounds.map(_.fold(
          x => x,
          x => x,
          { case (min, max) => max }))
        ).getOrElse(0))
      case Enum(ps) => Some(ps.head._2)
      case EnumSelect(_) => None
    }
  }

  private def satisfied(t: SQLServerTypeId, ts: SQLServerTypeId*) =
    TypeCoercion.Satisfied(NonEmptyList(t, ts.toList))
}
