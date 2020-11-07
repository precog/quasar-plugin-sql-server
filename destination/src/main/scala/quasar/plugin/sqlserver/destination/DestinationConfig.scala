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

import quasar.plugin.sqlserver.ConnectionConfig

import scala.StringContext
import java.lang.String

import argonaut._, Argonaut._

import cats._
import cats.implicits._

import quasar.plugin.jdbc.destination.WriteMode

final case class DestinationConfig(
    connectionConfig: ConnectionConfig,
    writeMode: WriteMode) {

  def jdbcUrl: String =
    connectionConfig.jdbcUrl

  def sanitized: DestinationConfig =
    copy(connectionConfig = connectionConfig.sanitized)
}

object DestinationConfig {
  implicit val destinationConfigCodecJson: CodecJson[DestinationConfig] =
    casecodec2(DestinationConfig.apply, DestinationConfig.unapply)("connection", "writeMode")

  implicit val destinationConfigEq: Eq[DestinationConfig] =
    Eq.by(c => (c.connectionConfig, c.writeMode))

  implicit val destinationConfigShow: Show[DestinationConfig] =
    Show.show(c => s"DestinationConfig(${c.connectionConfig.show}, ${c.writeMode.show})")
}
