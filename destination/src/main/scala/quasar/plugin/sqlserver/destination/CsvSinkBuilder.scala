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

import quasar.plugin.sqlserver._

import quasar.api.push.OffsetKey
import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.lib.jdbc._
import quasar.lib.jdbc.destination.{WriteMode => JWriteMode}

import cats.data.NonEmptyList
import cats.effect.Effect
import cats.implicits._

import doobie.{Fragment, Transactor}
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

import skolems.∀

private[destination] abstract class CsvSinkBuilder[F[_]: Effect: MonadResourceErr, Event[_]](
    xa: Transactor[F],
    writeMode: QWriteMode,
    jwriteMode: JWriteMode,
    scheme: String,
    path: ResourcePath,
    idColumn: Option[Column[SQLServerType]],
    val inputColumns: NonEmptyList[Column[SQLServerType]],
    logger: Logger) {

  val logHandler = Slf4sLogHandler(logger)

  def build: ∀[λ[α => Pipe[F, Event[OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] =
    ∀[λ[α => Pipe[F, Event[OffsetKey.Actual[α]], OffsetKey.Actual[α]]]](pipe)

  private def pipe[A](events: Stream[F, Event[OffsetKey.Actual[A]]]): Stream[F, OffsetKey.Actual[A]] = Stream.force {
    for {
      (objFragment, unsafeName, unsafeSchema) <- pathFragment[F](scheme, path)

      hygienicColumns = inputColumns.map { c =>
        (SQLServerHygiene.hygienicIdent(Ident(c.name)), c.tpe)
      }

      start = writeMode match {
        case QWriteMode.Replace =>
          startLoad(logHandler)(jwriteMode, objFragment, unsafeName, unsafeSchema, hygienicColumns, idColumn).transact(xa)
        case QWriteMode.Append =>
          ().pure[F]
      }

      logStart = trace(logger)("Starting load")
      logEnd = trace(logger)("Finished load")

      handled = events.evalTap(logEvents).through(handleEvents(objFragment, unsafeName)).unNone
    } yield Stream.eval_(logStart) ++ Stream.eval_(start) ++ handled ++ Stream.eval_(logEnd)
  }

  def logEvents(events: Event[_]): F[Unit]

  def handleEvents[A](objFragment: Fragment, unsafeName: String)
      : Pipe[F, Event[OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]]

  def trace(logger: Logger)(msg: => String): F[Unit] =
    Effect[F].delay(logger.trace(msg))
}

