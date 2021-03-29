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

import quasar.api.{Column, ColumnType}
import quasar.api.push.OffsetKey
import quasar.api.resource.ResourcePath
import quasar.connector.{AppendEvent, DataEvent, IdBatch, MonadResourceErr}
import quasar.connector.destination.{WriteMode => QWriteMode, ResultSink}, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc._
import quasar.lib.jdbc.destination.{WriteMode => JWriteMode}

import cats.data.{NonEmptyList, NonEmptyVector}
import cats.effect.{Effect, LiftIO}
import cats.syntax.all._

import doobie._
import doobie.free.connection.{commit, rollback}
import doobie.implicits._

import fs2.{Pipe, Stream}

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
    val consume = ∀[Consume[F, DataEvent[CharSequence, *], *]](upsertPipe(
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
    val consume = ∀[Consume[F, AppendEvent[CharSequence, *], *]](upsertPipe(
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

  private def upsertPipe[F[_]: Effect: MonadResourceErr, A](
      xa: Transactor[F],
      writeMode: QWriteMode,
      jwriteMode: JWriteMode,
      schema: String,
      path: ResourcePath,
      idColumn: Option[Column[SQLServerType]],
      inputColumns: NonEmptyList[Column[SQLServerType]],
      logger: Logger)
      : Pipe[F, DataEvent[CharSequence, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = { events =>

    val logHandler = Slf4sLogHandler(logger)
    val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]

    val (actualId, actualColumns) = idColumn match {
      case Some(c) => ensureIndexableIdColumn(c, inputColumns).leftMap(Some(_))
      case None => (None, inputColumns)
    }

    val hyColumns = hygienicColumns(actualColumns)

    def logEvents(event: DataEvent[CharSequence, _]): F[Unit] = event match {
      case DataEvent.Create(chunk) =>
        trace(logger)(s"Loading chunk with size: ${chunk.size}")
      case DataEvent.Delete(idBatch) =>
        trace(logger)(s"Deleting ${idBatch.size} records")
      case DataEvent.Commit(_) =>
        trace(logger)(s"Commit")
    }

    def handleEvents(obj: Fragment, unsafeName: String)
      : Pipe[ConnectionIO, DataEvent[CharSequence, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] = _ evalMap {

      case DataEvent.Create(chunk) =>
        insertChunk(logHandler)(obj, hyColumns, chunk)
          .as(none[OffsetKey.Actual[A]])

      case DataEvent.Delete(ids) if ids.size < 1 =>
        none[OffsetKey.Actual[A]].pure[ConnectionIO]

      case DataEvent.Delete(ids) => actualId match {
        case None =>
          none[OffsetKey.Actual[A]].pure[ConnectionIO]

        case Some(id) =>
          val columnName = SQLServerHygiene.hygienicIdent(Ident(id.name))

          val preamble: Fragment =
            fr"DELETE FROM" ++ obj ++ fr" WHERE" ++ Fragment.const(columnName.forSqlName)

          val deleteFragment: Fragment = ids match {
            case IdBatch.Strings(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
            case IdBatch.Longs(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
            case IdBatch.Doubles(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
            case IdBatch.BigDecimals(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
          }

          deleteFragment
            .updateWithLogHandler(logHandler)
            .run
            .as(none[OffsetKey.Actual[A]])
      }

      case DataEvent.Commit(offset) =>
        commit.as(offset).map(_.some)
    }

    def trace(logger: Logger)(msg: => String): F[Unit] =
      Effect[F].delay(logger.trace(msg))

    Stream.force {
      for {
        (objFragment, unsafeName, unsafeSchema) <- pathFragment[F](schema, path)

        start = writeMode match {
          case QWriteMode.Replace =>
            (startLoad(logHandler)(
              jwriteMode,
              objFragment,
              unsafeName,
              unsafeSchema,
              hyColumns,
              actualId) >> commit)
              .transact(xa)
          case QWriteMode.Append =>
            ().pure[F]
        }

        logStart = trace(logger)("Starting load")
        logEnd = trace(logger)("Finished load")

        translated = events.evalTap(logEvents).translate(toConnectionIO)
        events0 = translated.through(handleEvents(objFragment, unsafeName)).unNone
        rollback0 = Stream.eval(rollback).drain
        handled = (events0 ++ rollback0).transact(xa)
      } yield Stream.eval_(logStart) ++ Stream.eval_(start) ++ handled ++ Stream.eval_(logEnd)
    }
  }

  /** Ensures the provided identity column is suitable for indexing by SQL Server,
    * adjusting the type to one that is compatible and indexable if not.
    */
  private def ensureIndexableIdColumn(
      id: Column[SQLServerType],
      columns: NonEmptyList[Column[SQLServerType]])
      : (Column[SQLServerType], NonEmptyList[Column[SQLServerType]]) =
    Typer.inferScalar(id.tpe)
      .collect {
        case t @ ColumnType.String if id.tpe.some === Typer.preferred(t) =>
          val indexableId = id.as(SQLServerType.VARCHAR(MaxIndexableVarchars))
          val cols = columns.map(c => if (c === id) indexableId else c)
          (indexableId, cols)
      }
      .getOrElse((id, columns))
}
