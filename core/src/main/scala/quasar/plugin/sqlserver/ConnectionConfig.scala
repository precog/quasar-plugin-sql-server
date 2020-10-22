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

package quasar.plugin.sqlserver

import scala._, Predef._
import scala.concurrent.duration._

import argonaut._, Argonaut._, ArgonautCats._

import cats._
import cats.data.Validated
import cats.implicits._

import monocle.{Lens, Traversal}

import shims.monoidToScalaz

final case class ConnectionConfig(
    baseUrl: String,
    parameters: List[DriverParameter],
    maxConcurrency: Option[Int],
    maxLifetime: Option[FiniteDuration]) {

  import ConnectionConfig._

  def isSensitive: Boolean =
    Optics.parameterNames.exist(SensitiveParameters)(this)

  def jdbcUrl: String =
    if (parameters.isEmpty)
      baseUrl
    else
      s"$baseUrl;${parameters.map(_.forUrl).intercalate(";")}"

  def mergeSensitive(other: ConnectionConfig): ConnectionConfig = {
    val namesToMerge =
      SensitiveParameters.filterNot(name => parameters.exists(_.name == name))

    val paramsToMerge =
      Optics.driverParameters
        .foldMap(List(_).filter(p => namesToMerge(p.name)))(other)

    Optics.parameters.modify(paramsToMerge ::: _)(this)
  }

  def sanitized: ConnectionConfig =
    Optics.driverParameters
      .modify(p =>
        if (SensitiveParameters(p.name))
          DriverParameter.Optics.value.set(Redacted)(p)
        else
          p
      )(this)

  def validated: Validated[String, ConnectionConfig] = {
    val denied =
      Optics.parameterNames
        .getAll(this)
        .filter(DeniedParameters)

    Validated.cond(
      denied.isEmpty,
      this,
      denied.mkString("Unsupported parameters: ", ", ", ""))
  }
}

// applicationIntent=ReadOnly
// selectMethod=cursor
// sendTimeAsDatetime=false
// sendTemporalDataTypesAsStringForBulkCopy=true ????
// delayLoadingJobs
object ConnectionConfig {

  val Redacted = "--REDACTED--"

  val SensitiveParameters: Set[String] =
    Set(
      "clientKeyPassword",
      "gsscredential",
      "keyStoreSecret",
      "password",
      "trustStorePassword")

  val DeniedParameters: Set[String] =
    Set(
      "accessToken", // can't be set using a connection URL
      "cancelQueryTimeout",
      "delayLoadingJobs",
      "disableStatementPooling",
      "enablePrepareOnFirstPreparedStatementCall",
      "failoverPartner",
      "lastUpdateCount",
      "lockTimeout",
      "packetSize",
      "responseBuffering",
      "serverPreparedStatementDiscardThreshold",
      "statementPoolingCacheSize",
      "useBulkCopyForBatchInsert",
      "useFmtOnly",
      "xopenStates")

  object Optics {
    import shims.traverseToScalaz

    val baseUrl: Lens[ConnectionConfig, String] =
      Lens[ConnectionConfig, String](_.baseUrl)(u => _.copy(baseUrl = u))

    val parameters: Lens[ConnectionConfig, List[DriverParameter]] =
      Lens[ConnectionConfig, List[DriverParameter]](_.parameters)(ps => _.copy(parameters = ps))

    val driverParameters: Traversal[ConnectionConfig, DriverParameter] =
      parameters.composeTraversal(Traversal.fromTraverse[List, DriverParameter])

    val parameterNames: Traversal[ConnectionConfig, String] =
      driverParameters.composeLens(DriverParameter.Optics.name)

    val maxConcurrency: Lens[ConnectionConfig, Option[Int]] =
      Lens[ConnectionConfig, Option[Int]](_.maxConcurrency)(n => _.copy(maxConcurrency = n))

    val maxLifetime: Lens[ConnectionConfig, Option[FiniteDuration]] =
      Lens[ConnectionConfig, Option[FiniteDuration]](_.maxLifetime)(d => _.copy(maxLifetime = d))
  }

  implicit val connectionConfigCodecJson: CodecJson[ConnectionConfig] =
    CodecJson(
      cc =>
        ("jdbcUrl" := cc.jdbcUrl) ->:
        ("maxConcurrency" :=? cc.maxConcurrency) ->?:
        ("maxLifetimeSecs" :=? cc.maxLifetime.map(_.toSeconds)) ->?:
        jEmptyObject,

      cursor => for {
        maxConcurrency <- (cursor --\ "maxConcurrency").as[Option[Int]]
        maxLifetimeSecs <- (cursor --\ "maxLifetimeSecs").as[Option[Int]]
        maxLifetime = maxLifetimeSecs.map(_.seconds)

        urlCursor = cursor --\ "jdbcUrl"

        urlString <- urlCursor.as[String]

        queryStart = urlString.indexOf(';')

        (baseUrl, query) =
          if (queryStart < 0)
            (urlString, "")
          else
            (
              urlString.substring(0, queryStart),
              urlString.substring(queryStart + 1, urlString.length)
            )

        separated =
          if (query.isEmpty)
            Nil
          else
            query.split(';').toList

        params <- separated traverse {
          case DriverParameter.NameValue(name, value) =>
            DecodeResult.ok(DriverParameter(name, value))

          case _ =>
            DecodeResult.fail[DriverParameter]("Malformed driver parameter", urlCursor.history)
        }
      } yield ConnectionConfig(baseUrl, params, maxConcurrency, maxLifetime))

  implicit val connectionConfigEq: Eq[ConnectionConfig] =
    Eq.by(cc => (
      cc.baseUrl,
      cc.parameters))

  implicit val connectionConfigShow: Show[ConnectionConfig] =
    Show show { cc =>
      s"ConnectionConfig(${cc.jdbcUrl}, ${cc.maxConcurrency}, ${cc.maxLifetime})"
    }
}
