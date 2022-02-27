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

import scala.Boolean

import quasar.plugin.sqlserver._

import argonaut._, Argonaut._

final case class DatasourceConfig(connectionConfig: ConnectionConfig) {
  def isSensitive: Boolean =
    connectionConfig.isSensitive

  def mergeSensitive(other: DatasourceConfig): DatasourceConfig =
    copy(connectionConfig = connectionConfig.mergeSensitive(other.connectionConfig))

  def sanitized: DatasourceConfig =
    copy(connectionConfig = connectionConfig)
}

object DatasourceConfig {
  implicit val datasourceConfigCodecJson: CodecJson[DatasourceConfig] =
    casecodec1(DatasourceConfig.apply, DatasourceConfig.unapply)("connection")
}
