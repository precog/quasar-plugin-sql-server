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
import quasar.connector.MonadResourceErr
import quasar.connector.render.RenderConfig

import scala._, Predef._
import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

import quasar.lib.jdbc.destination.WriteMode

private[destination] object CsvCreateSink {
  def apply[F[_]: ConcurrentEffect: MonadResourceErr](
      writeMode: WriteMode,
      xa: Transactor[F],
      logger: Logger,
      schema: String)(
      path: ResourcePath,
      columns: NonEmptyList[Column[SQLServerType]])
      : (RenderConfig[CharSequence], Pipe[F, CharSequence, Unit]) = {

    val hyCols = hygienicColumns(columns)

    (renderConfig(columns), in => for {
      flow <- Stream.resource(TempTableFlow(xa, logger, writeMode, path, schema, hyCols, None, None))
      _ <- in.chunks.evalMap(x => flow.ingest(x).transact(xa))
      _ <- Stream.eval(flow.replace.transact(xa))
    } yield ())
  }
}
