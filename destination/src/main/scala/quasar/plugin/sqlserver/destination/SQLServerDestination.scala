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

import cats.effect.{ConcurrentEffect, Timer, Resource}

import doobie.Transactor

import monocle.Prism

import org.slf4s.Logger

import quasar.api.{ColumnType, Label}
import quasar.api.push.param.Actual
import quasar.api.push.{TypeCoercion, SelectedType, TypeIndex}
import quasar.api.push.TypeCoercion
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Constructor, Destination}
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{FlowSinks, FlowArgs, Flow, Retry}

import skolems.∃

import java.lang.CharSequence
import scala.concurrent.duration.FiniteDuration

private[destination] final class SQLServerDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](
    writeMode: WriteMode,
    schema: String,
    xa: Transactor[F],
    maxReattempts: Int,
    retryTimeout: FiniteDuration,
    logger: Logger)
    extends Destination[F] with FlowSinks[F, SQLServerType, CharSequence] {

  type Type = SQLServerType
  type TypeId = SQLServerTypeId

  val destinationType = SQLServerDestinationModule.destinationType

  def render(args: FlowArgs[SQLServerType]) = renderConfig(args.columns)

  def flowResource(args: FlowArgs[SQLServerType]): Resource[F, Flow[CharSequence]] =
    TempTableFlow(xa, logger, writeMode, schema, args) map { (f: Flow[CharSequence]) =>
      f.mapK(Retry[F](maxReattempts, retryTimeout))
    }

  val flowTransactor = xa
  val flowLogger = logger
  val sinks =
    flowSinks

  val typeIdOrdinal: Prism[Int, TypeId] =
    Prism(SQLServerDestination.OrdinalMap.get(_))(_.ordinal)

  val typeIdLabel: Label[TypeId] =
    Label.label[TypeId](_.toString)

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] =
    Typer.coerce(tpe)
        
  override def defaultSelected(tpe: ColumnType.Scalar): Option[SelectedType] = tpe match {
    case ColumnType.String =>
      SelectedType(TypeIndex(SQLServerType.VARCHAR.ordinal), List(∃(Actual.integer(256)))).some
    case _ =>
      none[SelectedType]
  }

  def construct(id: TypeId): Either[Type, Constructor[Type]] =
    Typer.construct(id)
}

object SQLServerDestination {
  val OrdinalMap: Map[Int, SQLServerTypeId] =
    SQLServerTypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
