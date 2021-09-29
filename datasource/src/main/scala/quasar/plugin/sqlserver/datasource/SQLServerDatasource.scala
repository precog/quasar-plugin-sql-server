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

package quasar.plugin.sqlserver.datasource

import quasar.plugin.sqlserver.{HI, SQLServerHygiene}

import scala._, Predef._
import scala.collection.immutable.Map
import java.lang.Throwable
import java.time.format.DateTimeFormatter

import cats.{Defer, Id}
import cats.data.NonEmptyList
import cats.effect.{Bracket, Resource}
import cats.implicits._

import doobie._
import doobie.implicits._

import quasar.api.DataPathSegment
import quasar.api.push.InternalKey
import quasar.connector.{MonadResourceErr, Offset}
import quasar.connector.datasource.{DatasourceModule, Loader}
import quasar.lib.jdbc._
import quasar.lib.jdbc.datasource._

import org.slf4s.Logger

import skolems.∃

private[datasource] object SQLServerDatasource {
  val DefaultResultChunkSize: Int = 4096

  def apply[F[_]: Bracket[?[_], Throwable]: Defer: MonadResourceErr](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      log: Logger)
      : DatasourceModule.DS[F] = {

    val maskInterpreter =
      MaskInterpreter(SQLServerHygiene) { (table, schema) =>
        discovery.tableColumns(table.asIdent, schema.map(_.asIdent))
          .map(m =>
            Mapping.SQLServerColumnTypes.get(m.vendorType)
             .orElse(Mapping.JdbcColumnTypes.get(m.jdbcType))
             .tupleLeft(m.name))
          .unNone
          .compile.to(Map)
      }

    val loader =
      JdbcLoader(xa, discovery, SQLServerHygiene) {
        RValueLoader.seek[HI](
          Slf4sLogHandler(log),
          DefaultResultChunkSize,
          SQLServerRValueColumn,
          offsetFragment)
        .compose(maskInterpreter.andThen(Resource.eval(_)))
      }

    JdbcDatasource(
      xa,
      discovery,
      SQLServerDatasourceModule.kind,
      NonEmptyList.one(Loader.Batch(loader)))
  }

  private def offsetFragment(offset: Offset): Either[String, Fragment] =
    for {
      ioffset <- internalize(offset)
      cfr <- columnFragment(ioffset)
      result <- makeOffset(cfr, ioffset.value)
    } yield result

  private def internalize(offset: Offset): Either[String, Offset.Internal] = offset match {
    case _: Offset.External =>
      Left("SQL Server datasource supports only internal offsets")
    case i: Offset.Internal =>
      i.asRight[String]
  }

  private def columnFragment(offset: Offset.Internal): Either[String, Fragment] =
    offset.path match {
      case NonEmptyList(DataPathSegment.Field(s), List()) =>
        Fragment.const(SQLServerHygiene.hygienicIdent(Ident(s)).forSql).asRight[String]
      case _ =>
        Left("Incorrect offset path")
    }

  private def LocalFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSSSSS")
  private def Formatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSSSSS xxxxx")
  private def DateFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd")

  private def makeOffset(colFr: Fragment, key: ∃[InternalKey.Actual]): Either[String, Fragment] = {
    val actual: InternalKey[Id, _] = key.value

    val actualFragment = actual match {
      case InternalKey.RealKey(k) =>
        Right(Fragment.const(k.toString))
      case InternalKey.StringKey(s) =>
        Right(fr0"'" ++ Fragment.const0(s.replace("'", "''")) ++ fr0"'")
      case InternalKey.DateTimeKey(d) =>
        Right {
          fr0"CAST('" ++
          Fragment.const0(Formatter.format(d)) ++
          fr0"' as datetimeoffset)"
        }
      case InternalKey.LocalDateTimeKey(d) =>
        Right {
          fr0"CAST('" ++
          Fragment.const0(LocalFormatter.format(d)) ++
          fr0"' as datetime2)"
        }
      case InternalKey.LocalDateKey(d) =>
        Right {
          fr0"CAST('" ++
          Fragment.const0(DateFormatter.format(d)) ++
          fr0"' as date)"
        }
      case InternalKey.DateKey(_) =>
        // Note that according to SQLServerRValueColumn and Mapping this is impossible
        Left("SQL Server doesn't support OffsetDate offsets")
    }
    actualFragment map { afr =>
      fr"$colFr >= $afr"
    }
  }

}
