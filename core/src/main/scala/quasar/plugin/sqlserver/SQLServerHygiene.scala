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

import scala.Predef._

import quasar.lib.jdbc.{Hygiene, Hygienic, Ident}

import cats.implicits._

object SQLServerHygiene extends Hygiene {
  final case class HygienicIdent(asIdent: Ident) extends Hygienic {
    def forSql: String =
      asIdent.asString.split('.').map('[' + _ + ']').toList.intercalate(".")

    def unsafeForSqlName: String = elideBrackets(asIdent.asString)

    def forSqlName: String = '[' + unsafeForSqlName + ']'
  }

  def hygienicIdent(ident: Ident): HygienicIdent =
    HygienicIdent(ident)

  def elideBrackets(input: String): String =
    input
      .replaceAllLiterally("[", "_")
      .replaceAllLiterally("]", "_")
}
