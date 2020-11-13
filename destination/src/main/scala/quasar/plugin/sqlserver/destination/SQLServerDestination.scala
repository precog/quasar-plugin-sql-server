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

import quasar.api.Column
import quasar.api.resource.ResourcePath

import scala._, Predef._

import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, Timer}

import doobie.Transactor

import monocle.Prism

import org.slf4s.Logger

import quasar.api.{ColumnType, Label}
import quasar.api.push.TypeCoercion
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Constructor, Destination, ResultSink}
import quasar.lib.jdbc.destination.WriteMode

private[destination] final class SQLServerDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](
    writeMode: WriteMode,
    schema: String,
    xa: Transactor[F],
    logger: Logger)
    extends Destination[F] {

  import SQLServerType._

  type Type = SQLServerType
  type TypeId = SQLServerTypeId

  val destinationType = SQLServerDestinationModule.destinationType

  def createSink(path: ResourcePath, columns: NonEmptyList[Column[SQLServerType]]) =
    CsvCreateSink[F](writeMode, xa, logger, schema)(path, columns)

  val sinks = NonEmptyList.one(ResultSink.create(createSink))

  val typeIdOrdinal: Prism[Int, TypeId] =
    Prism(SQLServerDestination.OrdinalMap.get(_))(_.ordinal)

  val typeIdLabel: Label[TypeId] =
    Label.label[TypeId](_.toString)

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] = {
    def satisfied(t: TypeId, ts: TypeId*) =
      TypeCoercion.Satisfied(NonEmptyList(t, ts.toList))

    tpe match {
      case ColumnType.Boolean => satisfied(BIT)

      case ColumnType.LocalTime => satisfied(TIME)
      case ColumnType.LocalDate => satisfied(DATE)
      case ColumnType.LocalDateTime => satisfied(DATETIME2, DATETIME, SMALLDATETIME)
      case ColumnType.OffsetDateTime => satisfied(DATETIMEOFFSET)

      case ColumnType.OffsetTime =>
        TypeCoercion.Unsatisfied(List(ColumnType.LocalTime), None)

      case ColumnType.OffsetDate =>
        TypeCoercion.Unsatisfied(List(ColumnType.LocalDate), None)

      case ColumnType.Number =>
        satisfied(
          FLOAT,
          INT,
          DECIMAL,
          BIGINT,
          NUMERIC,
          REAL,
          SMALLINT,
          TINYINT)

      case ColumnType.String =>
        satisfied(
          TEXT,
          NTEXT,
          NCHAR,
          NVARCHAR,
          CHAR,
          VARCHAR)

      case _ => TypeCoercion.Unsatisfied(Nil, None)
    }
  }

  def construct(id: TypeId): Either[Type, Constructor[Type]] =
    id match {
      case tpe: SQLServerTypeId.SelfIdentified => Left(tpe)
      case hk: SQLServerTypeId.HigherKinded => Right(hk.constructor)
    }
}

object SQLServerDestination {
  val OrdinalMap: Map[Int, SQLServerTypeId] =
    SQLServerTypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
