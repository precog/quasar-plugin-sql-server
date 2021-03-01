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

import quasar.api.push.OffsetKey
import quasar.connector.destination.ResultSink.UpsertSink
import quasar.connector.render.RenderConfig
import quasar.connector.{DataEvent, MonadResourceErr}
import quasar.lib.jdbc.destination.{WriteMode => JWriteMode}

import java.lang.CharSequence

import cats.effect.Effect
import cats.implicits._

import doobie._
import doobie.free.connection.{commit, rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.util.transactor.Strategy

import fs2.Pipe
import org.slf4s.Logger

import skolems.∀

private[destination] object CsvUpsertSink {
  type Consume[F[_], A] =
    Pipe[F, DataEvent[CharSequence, OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def builder[F[_]: Effect: MonadResourceErr](
      xa0: Transactor[F],
      writeMode: JWriteMode,
      args: UpsertSink.Args[SQLServerType],
      schema: String,
      logger: Logger)
      : CsvSinkBuilder[F, DataEvent[CharSequence, *]] = {
    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    new CsvSinkBuilder[F, DataEvent[CharSequence, *]](
      xa,
      args.writeMode,
      writeMode,
      schema,
      args.path,
      Some(args.idColumn),
      args.columns,
      logger) {

      val hyColumns = hygienicColumns(args.columns)

      def logEvents(event: DataEvent[CharSequence, _]): F[Unit] =
        ().pure[F]

      def handleEvents[A](obj: Fragment, unsafeName: String)
        : Pipe[F, DataEvent[CharSequence, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] = _ evalMap {
        case DataEvent.Create(chunk) =>
          insertChunk(logHandler)(obj, hyColumns, chunk)
            .transact(xa)
            .as(none[OffsetKey.Actual[A]])
        case DataEvent.Delete(ids) =>
          none[OffsetKey.Actual[A]].pure[F]
        case DataEvent.Commit(offset) =>
          commit.as(offset).map(_.some).transact(xa)
      }
    }
  }

  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: JWriteMode,
      schema: String,
      logger: Logger)(
      args: UpsertSink.Args[SQLServerType])
      : (RenderConfig[CharSequence], ∀[Consume[F, *]]) =
    (renderConfig(args.columns), builder(xa, writeMode, args, schema, logger).build)
}
