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
import quasar.connector.{AppendEvent, DataEvent, MonadResourceErr}
import quasar.connector.destination.ResultSink, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.{WriteMode => JWriteMode}

import cats.effect.Effect

import doobie.Transactor

import fs2.Pipe

import java.lang.CharSequence

import org.slf4s.Logger

import skolems.∀

object SinkBuilder {
  type Consume[F[_], Event[_], A] =
    Pipe[F, Event[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def upsert[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: JWriteMode,
      schema: String,
      logger: Logger)(
      args: UpsertSink.Args[SQLServerType])
      : (RenderConfig[CharSequence], ∀[Consume[F, DataEvent[CharSequence, *], *]]) = {
    val consume = ∀[Consume[F, DataEvent[CharSequence, *], *]](UpsertPipe(
      xa,
      args.writeMode,
      writeMode,
      schema,
      args.path,
      Some(args.idColumn),
      args.columns,
      logger))
    (renderConfig(args.columns), consume)
  }

  def append[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: JWriteMode,
      schema: String,
      logger: Logger)(
      args: AppendSink.Args[SQLServerType])
      : (RenderConfig[CharSequence], ∀[Consume[F, AppendEvent[CharSequence, *], *]]) = {
    val consume = ∀[Consume[F, AppendEvent[CharSequence, *], *]](UpsertPipe(
      xa,
      args.writeMode,
      writeMode,
      schema,
      args.path,
      args.pushColumns.primary,
      args.columns,
      logger))
    (renderConfig(args.columns), consume)
  }
}
