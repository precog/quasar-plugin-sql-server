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

import scala._
import scala.concurrent.duration._

import argonaut._, Argonaut._

import org.specs2.mutable.Specification

// "jdbc:sqlserver://localhost:1433;user=SA;password=<YourStrong@Passw0rd>;database=TestDB"
object ConnectionConfigSpec extends Specification {

  import ConnectionConfig.Redacted

  "serialization" >> {
    "valid config" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sqlserver://localhost:1433;user=SA;password=<YourStrong@Passw0rd>;database=TestDB",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("user", "SA"),
            DriverParameter("password", "<YourStrong@Passw0rd>"),
            DriverParameter("database", "TestDB")),
          Some(4),
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "url parameters are optional" >> {
      val base = "jdbc:sqlserver://1.2.3.4:1234"

      val js = s"""
        {
          "jdbcUrl": "$base",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          base,
          Nil,
          Some(4),
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "max concurrency is optional" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sqlserver://localhost:1433;user=SA;password=<YourStrong@Passw0rd>;database=TestDB",
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("user", "SA"),
            DriverParameter("password", "<YourStrong@Passw0rd>"),
            DriverParameter("database", "TestDB")),
          None,
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "max lifetime is optional" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sqlserver://localhost:1433;user=SA;password=<YourStrong@Passw0rd>;database=TestDB",
          "maxConcurrency": 4
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("user", "SA"),
            DriverParameter("password", "<YourStrong@Passw0rd>"),
            DriverParameter("database", "TestDB")),
          Some(4),
          None)

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "fails when parameters malformed" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sqlserver://localhost:1433;=SA;password=<YourStrong@Passw0rd>;database=TestDB",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      js.decodeEither[ConnectionConfig] must beLeft(contain("Malformed driver parameter"))
    }
  }

  "validation" >> {
    "fails when a denied parameter is present" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("userName", "bob"),
            DriverParameter("lockTimeout", "1000")),
          Some(3),
          None)

      cc.validated.toEither must beLeft("Unsupported parameters: lockTimeout")
    }

    "fails when multiple denied parameters are present" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("lastUpdateCount", "true"),
            DriverParameter("userName", "bob"),
            DriverParameter("lockTimeout", "1000")),
          Some(3),
          None)

      cc.validated.toEither must beLeft("Unsupported parameters: lastUpdateCount, lockTimeout")
    }

    "succeeds when no parameters are denied" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sqlserver://localhost:1433",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("userName", "bob"),
            DriverParameter("databaseName", "xyzdb")),
          Some(3),
          None)

      cc.validated.toEither must beRight(cc)
    }
  }

  "merge sensitive" >> {
    "merges undefined sensitive params from other" >> {
      val a = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("userName", "alice"),
          DriverParameter("databaseName", "db1")),
        None,
        None)

      val b = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("userName", "bob"),
          DriverParameter("password", "secret"),
          DriverParameter("databaseName", "db2")),
        None,
        None)

      val expected = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("password", "secret"),
          DriverParameter("userName", "alice"),
          DriverParameter("databaseName", "db1")),
        None,
        None)

      a.mergeSensitive(b) must_=== expected
    }

    "retains local sensitive params" >> {
      val a = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("userName", "alice"),
          DriverParameter("password", "toor"),
          DriverParameter("databaseName", "db1")),
        None,
        None)

      val b = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("userName", "bob"),
          DriverParameter("password", "secret"),
          DriverParameter("keyStoreSecret", "hiddenkeys"),
          DriverParameter("databaseName", "db2")),
        None,
        None)

      val expected = ConnectionConfig(
        "jdbc:sqlserver://localhost:1433",
        List(
          DriverParameter("keyStoreSecret", "hiddenkeys"),
          DriverParameter("userName", "alice"),
          DriverParameter("password", "toor"),
          DriverParameter("databaseName", "db1")),
        None,
        None)

      a.mergeSensitive(b) must_=== expected
    }
  }
}

