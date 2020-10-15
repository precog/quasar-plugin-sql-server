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

import scala._, Predef._

import argonaut._, Argonaut._

import org.specs2.mutable.Specification

import quasar.api.datasource.DatasourceError
import quasar.connector.datasource.Reconfiguration

object SQLServerDatasourceModuleSpec extends Specification {
  "reconfiguration" >> {
    "fails if original is malformed" >> {
      val orig =
        "[1, 2, 3]".parseOption.get

      val patch =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433"}}""".parseOption.get

      SQLServerDatasourceModule.reconfigure(orig, patch) must beLeft.like {
        case DatasourceError.MalformedConfiguration(_, _, msg) =>
          msg must contain("original")
      }
    }

    "fails if patch is malformed" >> {
      val orig =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433"}}""".parseOption.get

      val patch =
        "[1, 2, 3]".parseOption.get

      SQLServerDatasourceModule.reconfigure(orig, patch) must beLeft.like {
        case DatasourceError.MalformedConfiguration(_, _, msg) =>
          msg must contain("new")
      }
    }

    "fails if patch is sensitive" >> {
      val orig =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433"}}""".parseOption.get

      val patch =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433;password=root"}}""".parseOption.get

      SQLServerDatasourceModule.reconfigure(orig, patch) must beLeft.like {
        case DatasourceError.InvalidConfiguration(_, _, msgs) =>
          msgs.head must contain("sensitive")
      }
    }

    "merges sensitive information from original" >> {
      val orig =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433;password=secret"}}""".parseOption.get

      val patch =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433;userName=alice"}}""".parseOption.get

      val expected =
        """{"connection": {"jdbcUrl": "jdbc:sqlserver://localhost:1433;password=secret;userName=alice"}}""".parseOption.get

      SQLServerDatasourceModule.reconfigure(orig, patch) must beRight(Reconfiguration.Reset -> expected)
    }
  }
}
