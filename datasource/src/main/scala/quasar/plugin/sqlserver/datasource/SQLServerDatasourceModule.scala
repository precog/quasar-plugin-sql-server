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

import scala._
import scala.Predef._

import quasar.RateLimiting
import quasar.api.datasource.DatasourceError.ConfigurationError
import quasar.api.datasource.DatasourceType
import quasar.connector.{ByteStore, ExternalCredentials, MonadResourceErr}
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}
import quasar.plugin.jdbc.TransactorConfig
import quasar.plugin.jdbc.datasource.JdbcDatasourceModule

import java.util.UUID

import argonaut.Json

import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import doobie.Transactor

import org.slf4s.Logger

// adaptive buffering:
// https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-ver15
object SQLServerDbDatasourceModule extends JdbcDatasourceModule[DatasourceConfig] {

  def jdbcDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A](
      config: DatasourceConfig,
      transactor: Transactor[F],
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      getAuth: UUID => F[Option[ExternalCredentials[F]]],
      log: Logger)
      : Resource[F, Either[SQLServerDbDatasourceModule.InitError, LightweightDatasourceModule.DS[F]]] = ???

  def transactorConfig(config: DatasourceConfig)
      : Either[NonEmptyList[String], TransactorConfig] = ???

  def kind: DatasourceType = DatasourceType("sql-server", 1L)

  def migrateConfig[F[_]: Sync](config: argonaut.Json)
      : F[Either[ConfigurationError[Json], Json]] = ???

  def reconfigure(original: Json ,patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)] = ???

  def sanitizeConfig(config: argonaut.Json): argonaut.Json = ???
}
