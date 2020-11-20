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

import quasar.plugin.sqlserver._

import quasar.api.Column
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.render.RenderConfig

import scala._, Predef._
import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

import quasar.lib.jdbc
import quasar.lib.jdbc.{Ident, Slf4sLogHandler}
import quasar.lib.jdbc.destination.WriteMode

private[destination] object CsvCreateSink {
  def apply[F[_]: ConcurrentEffect](
      writeMode: WriteMode,
      xa: Transactor[F],
      logger: Logger,
      schema: String)(
      path: ResourcePath,
      columns: NonEmptyList[Column[SQLServerType]])
      : (RenderConfig[CharSequence], Pipe[F, CharSequence, Unit]) = {

    val logHandler = Slf4sLogHandler(logger)

    val renderConfig = RenderConfig.Separated(",", SQLServerColumnRender(columns))

    val hygienicColumns: NonEmptyList[(HI, SQLServerType)] =
      columns.map(c => (SQLServerHygiene.hygienicIdent(Ident(c.name)), c.tpe))

    val pathWithSchema = path./:(ResourceName(schema))
    val obj = jdbc.resourcePathRef(pathWithSchema).get // FIXME

    val objFragment: Fragment = obj.fold(
      t => Fragment.const0(SQLServerHygiene.hygienicIdent(t).forSqlName),
      { case (d, t) =>
        Fragment.const0(SQLServerHygiene.hygienicIdent(d).forSqlName) ++
          fr0"." ++
          Fragment.const0(SQLServerHygiene.hygienicIdent(t).forSqlName)
      })

    val unsafeTableName: String = obj.fold(
      t => SQLServerHygiene.hygienicIdent(t).unsafeForSqlName,
      { case (_, t) => SQLServerHygiene.hygienicIdent(t).unsafeForSqlName})

    val unsafeTableSchema: Option[String] = obj.fold(
      _ => None,
      { case (d, _) => Some(SQLServerHygiene.hygienicIdent(d).unsafeForSqlName) })

    def doLoad(obj: Fragment): Pipe[F, CharSequence, Unit] = in => {
      def insert(prefix: StringBuilder, length: Int): Stream[F, Unit] =
        Stream.eval(startLoad.transact(xa)) ++
          in.chunks.evalMap(insertChunk(logHandler)(obj, hygienicColumns, _).transact(xa))

      val (prefix, length) = insertIntoPrefix(logHandler)(obj, hygienicColumns)

      insert(prefix, length)
    }

    def startLoad: ConnectionIO[Unit] =
      for {
        _ <- writeMode match {
          case WriteMode.Create =>
            createTable(logHandler)(objFragment, hygienicColumns)

          case WriteMode.Replace =>
            replaceTable(logHandler)(objFragment, hygienicColumns)

          case WriteMode.Truncate =>
            truncateTable(logHandler)(objFragment, unsafeTableName, unsafeTableSchema, hygienicColumns)

          case WriteMode.Append =>
            appendToTable(logHandler)(objFragment, unsafeTableName, unsafeTableSchema, hygienicColumns)
        }
      } yield ()

    (renderConfig, in => Stream.eval(objFragment.pure[F]) flatMap {
      doLoad(_)(in)
    })
  }
}
