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
import scala.util.matching.Regex

import cats.{Eq, Show}
import cats.implicits._

import monocle.Lens

final case class DriverParameter(name: String, value: String) {
  def forUrl: String = s"$name=$value"
}

object DriverParameter {
  val NameValue: Regex = "^([^=]+)=(.*)$".r

  object Optics {
    val name: Lens[DriverParameter, String] =
      Lens[DriverParameter, String](_.name)(n => _.copy(name = n))

    val value: Lens[DriverParameter, String] =
      Lens[DriverParameter, String](_.value)(v => _.copy(value = v))
  }

  implicit val driverPropertyEq: Eq[DriverParameter] =
    Eq.by(dp => (dp.name, dp.value))

  implicit val driverPropertyShow: Show[DriverParameter] =
    Show.show(_.forUrl)
}

