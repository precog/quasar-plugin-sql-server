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

import cats.{~>, MonadError}
import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, Timer, Effect, LiftIO}
import cats.implicits._

import doobie.{ConnectionIO, Transactor}
import doobie.implicits._

import monocle.Prism

import org.slf4s.Logger

import quasar.api.{ColumnType, Label}
import quasar.api.push.TypeCoercion
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Constructor, Destination, ResultSink}
import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.duration.FiniteDuration

private[destination] final class SQLServerDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](
    writeMode: WriteMode,
    schema: String,
    xa: Transactor[F],
    maxReattempts: Int,
    retryTimeout: FiniteDuration,
    logger: Logger)
    extends Destination[F] {

  type Type = SQLServerType
  type TypeId = SQLServerTypeId

  val destinationType = SQLServerDestinationModule.destinationType

  val retry = SQLServerDestination.retryN(maxReattempts, retryTimeout, 0)

  val sinks = NonEmptyList.of(
    ResultSink.create(CsvCreateSink(writeMode, xa, retry, logger, schema)),
    ResultSink.upsert(SinkBuilder.upsert(xa, writeMode, schema, retry, logger)),
    ResultSink.append(SinkBuilder.append(xa, writeMode, schema, retry, logger)))

  val typeIdOrdinal: Prism[Int, TypeId] =
    Prism(SQLServerDestination.OrdinalMap.get(_))(_.ordinal)

  val typeIdLabel: Label[TypeId] =
    Label.label[TypeId](_.toString)

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] =
    Typer.coerce(tpe)

  def construct(id: TypeId): Either[Type, Constructor[Type]] =
    Typer.construct(id)
}

object SQLServerDestination {
  private def retryN[F[_]: Effect: Timer](maxN: Int, timeout: FiniteDuration, n: Int): ConnectionIO ~> ConnectionIO =
    λ[ConnectionIO ~> ConnectionIO] { action =>
      val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]
      action.attempt flatMap {
        case Right(a) => a.pure[ConnectionIO]
        case Left(e) if n < maxN =>
          toConnectionIO(Timer[F].sleep(timeout)) >>
          retryN[F](maxN, timeout, n + 1).apply(action)
        case Left(e) => MonadError[ConnectionIO, Throwable].raiseError(e)
      }
    }

  val OrdinalMap: Map[Int, SQLServerTypeId] =
    SQLServerTypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
