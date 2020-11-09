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

import quasar.plugin.jdbc.Ident
import quasar.plugin.sqlserver.{HI, SQLServerHygiene, TestHarness}

import scala.{text => _, Stream => _, _}, Predef._

import java.time._

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import doobie._
import doobie.implicits._
import doobie.implicits.javatime._

import fs2._

import org.slf4s.Logging

import quasar.api.Column
import quasar.api.resource._
import quasar.plugin.jdbc.destination.WriteMode

object SQLServerDestinationSpec extends TestHarness with Logging {
  import SQLServerType._

  def createSink(
      dest: SQLServerDestination[IO],
      path: ResourcePath,
      cols: NonEmptyList[Column[SQLServerType]])
      : Pipe[IO, Byte, Unit] = {
    println(s"path in createSink: $path")
    dest.createSink.consume(path, cols)._2
  }

  def harnessed(
      jdbcUrl: String = TestUrl(Some(TestDb)),
      writeMode: WriteMode = WriteMode.Replace,
      schema: String = "dbo")
      : Resource[IO, (Transactor[IO], SQLServerDestination[IO], ResourcePath, String)] =
    tableHarness(jdbcUrl, schema) map {
      case (xa, path, name) => {
        (xa, new SQLServerDestination(writeMode, schema, xa, log), path, name)
      }
    }

  def csv(lines: String*): Stream[IO, Byte] =
    Stream.emits(lines)
      .intersperse("\r\n")
      .through(text.utf8Encode)

  def ingestValues[A: Read](tpe: SQLServerType, values: A*) = {
    val input = csv(values.map(_.toString): _*)
    val cols = NonEmptyList.one(Column("value", tpe))

    harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
      for {
        _ <- input.through(createSink(dest, path, cols)).compile.drain
        _ = println(s"tableName: $tableName")
        _ = println(s"path in harnessed: $path")
        vals <- frag(s"select value from $tableName").query[A].to[List].transact(xa)
      } yield {
        vals must containTheSameElementsAs(values)
      }
    }
  }

  /*
  "write mode" >> {
    val cols = NonEmptyList.one(Column("value", CHAR(1)))

    "create" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          csv("A", "B")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "fails when table present" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          for {
            _ <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain
            r <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beLeft
          }
        }
      }
    }

    "replace" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          csv("A", "B")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          for {
            _ <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain
            r <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "truncate" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          csv("A", "B")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          for {
            _ <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain
            r <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "append" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          csv("A", "B")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          for {
            _ <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain
            r <- csv("A", "B").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }
  }
*/

  "ingest" >> {
/*
    "boolean" >> {
      val input = csv("1", "0")
      val cols = NonEmptyList.one(Column("thebool", BIT))

      harnessed() use { case (xa, dest, path, tableName) =>
        println(s"tablename: $tableName in boolean")
        println(s"path: $path in boolean")
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          bools <-
            frag(s"select thebool from $tableName")
              .query[Boolean].to[List].transact(xa)
        } yield {
          bools must contain(true, false)
        }
      }
    }
    */

    "number" >> {

      "tinyint" >> ingestValues(TINYINT, 0, 255)

      /*
      "smallint" >> ingestValues(SMALLINT(SIGNED), -32768, 0, 32767)

      "mediumint" >> ingestValues(MEDIUMINT(SIGNED), -8388608, 0, 8388607)

      "int" >> ingestValues(INT(SIGNED), -2147483648, 0, 2147483647)

      "bigint" >> ingestValues(BIGINT(SIGNED), -9223372036854775808L, 0L, 9223372036854775807L)

      "float" >> ingestValues(FLOAT(SIGNED), -3.40282E+38f, Float.MinPositiveValue, 3.40282E+38f)

      "double" >> ingestValues(DOUBLE(SIGNED), -1.7976931348623157E+308, 2.2250738585072014E-308, 1.7976931348623157E+308)

      "decimal" >> {
        val min = BigDecimal("-99999999999999999999999999999999999.999999999999999999999999999999")
        val mid = BigDecimal("1234567898765432100000000.000000000099999966999977777799")
        val max = BigDecimal("99999999999999999999999999999999999.999999999999999999999999999999")
        ingestValues(DECIMAL(65, 30, SIGNED), min, mid, max)
      }

      "year" >> ingestValues(YEAR, 1901, 1969, 2155)
      */
    }
    /*

    "string" >> {
      List(TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT) foreach { tpe =>
        tpe.toString.toLowerCase >> ingestValues(tpe, "føobår", "ひらがな", "b a t")
      }

      "nchar" >> ingestValues(CHAR(6), "føobår", "  ひらがな", " b a t")

      "nvarchar" >> ingestValues(VARCHAR(6), "føobår", "ひらがな", "b a t")

      "binary" >> ingestValues(BINARY(6), "foobar", "ひら", "føoba")

      "varbinary" >> ingestValues(VARBINARY(6), "foobar", "ひら", "føo")
    }

    "localdate" >> {
      val min = LocalDate.parse("1000-01-01")
      val mid = LocalDate.parse("2020-07-15")
      val max = LocalDate.parse("9999-12-31")

      ingestValues(DATE, min, mid, max)
    }

    "localtime" >> {
      val min = LocalTime.parse("00:00:00")
      val mid = LocalTime.parse("12:11:32")
      val midS = LocalTime.parse("13:42:32.123456")
      val max = LocalTime.parse("23:59:59.999999")

      ingestValues(TIME(6), min, mid, midS, max)
    }

    "localdatetime" >> {
      val min = LocalDateTime.parse("1000-01-01T00:00:00.000000")
      val mid = LocalDateTime.parse("2020-07-15T11:30:05")
      val midS = LocalDateTime.parse("2020-05-05T03:30:45")
      val max = LocalDateTime.parse("9999-12-31T23:59:59.999999")

      ingestValues(DATETIME(6), min, mid, midS, max)
    }

    "containing special characters" >> {
      val escA = """"foo,"",,""""""""
      val escB = """"java""script""""""

      val A = "foo,\",,\"\""
      val B = "java\"script\""

      val input = csv(escA, escB)
      val cols = NonEmptyList.one(Column("value", VARCHAR(12)))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select value from $tableName")
              .query[String].to[List].transact(xa)
        } yield {
          vals must contain(A, B)
        }
      }
    }

    "multiple fields" >> {
      val row1 = "2020-07-12,123456.234234,\"hello world\",887798"
      val row2 = "1983-02-17,732345.987,\"lorum, ipsum\",42"

      val input = csv(row1, row2)

      val cols = NonEmptyList.of(
        Column("A", DATE),
        Column("B", DOUBLE(SIGNED)),
        Column("C", VARCHAR(20)),
        Column("D", MEDIUMINT(SIGNED)))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select A, B, C, D from $tableName")
              .query[(LocalDate, Double, String, Int)].to[List].transact(xa)
        } yield {
          vals must contain(
            (LocalDate.parse("2020-07-12"), 123456.234234, "hello world", 887798),
            (LocalDate.parse("1983-02-17"), 732345.987, "lorum, ipsum", 42))

        }
      }
    }

    "undefined fields" >> {
      val row1 = "NULL,42,1992-03-04"
      val row2 = "foo,NULL,1992-03-04"
      val row3 = "foo,42,NULL"

      val input = csv(row1, row2, row3)

      val cols = NonEmptyList.of(
        Column("A", CHAR(3)),
        Column("B", TINYINT(UNSIGNED)),
        Column("C", DATE))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select A, B, C from $tableName")
              .query[(Option[String], Option[Int], Option[LocalDate])]
              .to[List].transact(xa)
        } yield {
          vals must contain(
            (None, Some(42), Some(LocalDate.parse("1992-03-04"))),
            (Some("foo"), None, Some(LocalDate.parse("1992-03-04"))),
            (Some("foo"), Some(42), None))
        }
      }
    }
*/
  }
}
