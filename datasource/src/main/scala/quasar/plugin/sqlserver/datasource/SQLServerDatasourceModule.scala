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

import quasar.RateLimiting
import quasar.api.datasource.{DatasourceType, DatasourceError}
import quasar.api.datasource.DatasourceError.ConfigurationError
import quasar.connector.{ByteStore, ExternalCredentials, MonadResourceErr}
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}
import quasar.plugin.jdbc.{JdbcDiscovery, JdbcDriverConfig, TableType, TransactorConfig}
import quasar.plugin.jdbc.datasource.JdbcDatasourceModule
import quasar.plugin.sqlserver.ConnectionConfig

import scala._
import scala.Predef._
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

import java.net.URI
import java.util.UUID

import argonaut._, Argonaut._, ArgonautCats._

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._

import doobie._

import org.slf4s.Logger

import scalaz.{NonEmptyList => ZNel}

// TODO comment on which param is adaptive by default in the config
// adaptive buffering:
// https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-ver15
object SQLServerDatasourceModule extends JdbcDatasourceModule[DatasourceConfig] {

  val DefaultConnectionMaxConcurrency: Int = 8
  val DefaultConnectionMaxLifetime: FiniteDuration = 5.minutes

  def jdbcDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A](
      config: DatasourceConfig,
      transactor: Transactor[F],
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      getAuth: UUID => F[Option[ExternalCredentials[F]]],
      log: Logger)
      : Resource[F, Either[SQLServerDatasourceModule.InitError, LightweightDatasourceModule.DS[F]]] = {

    val discovery = JdbcDiscovery(discoverableTableTypes(log))

    SQLServerDatasource(transactor, discovery, log)
      .asRight[InitError]
      .pure[Resource[F, ?]]
  }

  def discoverableTableTypes(log: Logger): Option[ConnectionIO[NonEmptySet[TableType]]] =
    Some(for {
      catalog <- HC.getCatalog
      rs <- HC.getMetaData(FDMD.getTableTypes)
      names <- FC.embed(rs, HRS.build[SortedSet, String])
      _ <- FC.delay(log.debug(s"AVAILABLE TABLE TYPES: ${names.toList.mkString(", ")}"))
      pruned = names.filterNot(n => n == "SYSTEM TABLE" || n == "SYSTEM VIEW")
      default = NonEmptySet.of("TABLE", "VIEW")
      discoverable = NonEmptySet.fromSet(pruned) getOrElse default
    } yield discoverable.map(TableType(_)))

  def transactorConfig(config: DatasourceConfig)
      : Either[NonEmptyList[String], TransactorConfig] =
    for {
      cc <- config.connectionConfig.validated.leftMap(NonEmptyList.one(_)).toEither

      jdbcUrl <-
        Either.catchNonFatal(new URI(cc.jdbcUrl))
          .leftMap(_ => NonEmptyList.one("JDBC URL is not a valid URI"))

      maxConcurrency = cc.maxConcurrency getOrElse DefaultConnectionMaxConcurrency
      maxLifetime = cc.maxLifetime getOrElse DefaultConnectionMaxLifetime
    } yield {
      TransactorConfig
        .withDefaultTimeouts(
          JdbcDriverConfig.JdbcDriverManagerConfig(
            jdbcUrl,
            Some("com.microsoft.sqlserver.jdbc.SQLServerDriver")),
          connectionMaxConcurrency = maxConcurrency,
          connectionReadOnly = true)
        .copy(connectionMaxLifetime = maxLifetime)
    }

  def kind: DatasourceType = DatasourceType("sql-server", 1L)

  def migrateConfig[F[_]: Sync](config: argonaut.Json)
      : F[Either[ConfigurationError[Json], Json]] =
    Sync[F].pure(Right(config))

  def reconfigure(original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)] = {
    def decodeCfg(js: Json, name: String): Either[ConfigurationError[Json], DatasourceConfig] =
      js.as[DatasourceConfig].toEither.leftMap { case (m, c) =>
        DatasourceError.MalformedConfiguration(
          kind,
          jString(ConnectionConfig.Redacted),
          s"Failed to decode $name config JSON at ${c.toList.map(_.show).mkString(", ")}")
      }

    for {
      prev <- decodeCfg(original, "original")
      next <- decodeCfg(patch, "new")

      result <-
        if (next.isSensitive)
          Left(DatasourceError.InvalidConfiguration(
            kind,
            next.sanitized.asJson,
            ZNel("New configuration contains sensitive information.")))
        else
          Right(next.mergeSensitive(prev))

    } yield (Reconfiguration.Reset, result.asJson)
  }

  def sanitizeConfig(config: Json): Json =
    config.as[DatasourceConfig].toOption
      .fold(jEmptyObject)(_.sanitized.asJson)
}
