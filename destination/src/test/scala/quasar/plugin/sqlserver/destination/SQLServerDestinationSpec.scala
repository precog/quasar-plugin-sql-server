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

  sequential // FIXME we need to be able to run multiple pushes at the same time

  def createSink(
      dest: SQLServerDestination[IO],
      path: ResourcePath,
      cols: NonEmptyList[Column[SQLServerType]])
      : Pipe[IO, Byte, Unit] =
    dest.createSink.consume(path, cols)._2

  def harnessed(
      jdbcUrl: String = TestUrl(Some(TestDb)),
      writeMode: WriteMode = WriteMode.Replace,
      schema: String = "dbo")
      : Resource[IO, (Transactor[IO], SQLServerDestination[IO], ResourcePath, String)] =
    tableHarness(jdbcUrl, false) map {
      case (xa, path, name) => {
        (xa, new SQLServerDestination(writeMode, schema, xa, log), path, name)
      }
    }

  def csv(lines: String*): Stream[IO, Byte] =
    Stream.emits(lines)
      .intersperse("\r\n")
      .through(text.utf8Encode)

  def ingestValues[A: Read](tpe: SQLServerType, values: A*) =
    ingestValuesWithExpected(tpe, values: _*)(values: _*)

  def ingestValuesWithExpected[A: Read](tpe: SQLServerType, values: A*)(expected: A*) = {
    val input = csv(values.map(_.toString): _*)
    val cols = NonEmptyList.one(Column("value", tpe))

    harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
      for {
        _ <- input.through(createSink(dest, path, cols)).compile.drain
        //_ = println(s"tableName: $tableName")
        //_ = println(s"path in harnessed: $path")
        vals <- frag(s"select value from $tableName").query[A].to[List].transact(xa)
      } yield {
        vals must containTheSameElementsAs(expected)
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
    "boolean" >> {
      val input = csv("1", "0")
      val cols = NonEmptyList.one(Column("thebool", BIT))

      harnessed() use { case (xa, dest, path, tableName) =>
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

    "number" >> {

      "tinyint" >> ingestValues(TINYINT, 0, 255)

      "smallint" >> ingestValues(SMALLINT, -32768, 0, 32767)

      "int" >> ingestValues(INT, -2147483648, 0, 2147483647)

      "bigint" >> ingestValues(BIGINT, -9223372036854775808L, 0L, 9223372036854775807L)

      "float" >> ingestValues(FLOAT(53), -1.7976931348623157E+308, -2.23E-308, 0, 2.23E-308, 1.7976931348623157E+308, Float.MinPositiveValue)

      "real" >> ingestValues(REAL, -3.3999999521443642E38, -1.179999945774631E-38, 0.0, 1.179999945774631E-38, 3.3999999521443642E38)

      "decimal" >> {
        val min = BigDecimal("-9999999999999999999999999999.9999999999")
        val mid = BigDecimal("1234499959999999999999999999.0001239999")
        val max = BigDecimal("9999999999999999999999999999.9999999999")
        ingestValues(DECIMAL(38, 10), min, mid, max)
      }

      "numeric" >> {
        val min = BigDecimal("-999999999999999999999999999999999999.99")
        val mid = BigDecimal("111112349999999999999999999999999999.12")
        val max = BigDecimal("999999999999999999999999999999999999.99")
        ingestValues(DECIMAL(38, 2), min, mid, max)
      }

      // FIXME why no money?!
      //"money" >> ingestValues(MONEY, BigDecimal(-922337203685477.5808), BigDecimal(0), BigDecimal(922337203685477.5807))
      //"smallmoney" >> ingestValues(SMALLMONEY, -214748.3648, 0, 214748.3647)
    }

    // FIXME why no unicode!?
    // char, nchar, nvarchar, text, uniqueid, varchar
    //"string" >> {
      //"text" >> ingestValues(TEXT, "føobår", "  ひらがな", " b a t")

      //"char" >> ingestValues(CHAR(6), "føobår", "  ひらがな", " b a t")

      //"varchar" >> ingestValues(VARCHAR(6), "føobår", "  ひらがな", " b a t")

      //"nchar" >> ingestValues(NCHAR(6), "føobår", "  ひらがな", " b a t")

      //"nvarchar" >> ingestValues(NVARCHAR(6), "føobår", "ひらがな", "b a t")
    //}

    "temporal" >> {
      "date" >> {
        val minDate = "0001-01-01"
        val midDate = "2020-11-09"
        val maxDate = "9999-12-31"

        ingestValues(DATE, minDate, midDate, maxDate)
      }

      "time" >> {
        val minTime = "00:00:00.000000"
        val midTime = "11:13:52.738493"
        val midTimeNoMs = "11:13:52"
        val maxTime = "23:59:59.000000"

        ingestValuesWithExpected(TIME(6), minTime, midTime, midTimeNoMs, maxTime)(
          minTime, midTime, "11:13:52.000000", maxTime)
      }

      "datetime" >> {
        val minDateTime = "1753-01-01T00:00:00.000"
        val midDateTime = "2020-11-09T11:13:38.742"
        val midDateTimeNoMs = "2020-11-09T11:13:38"
        val maxDateTime = "9999-12-31T23:59:59.997"

        val expectedMinDateTime = "1753-01-01 00:00:00.0"
        val expectedMidDateTime = "2020-11-09 11:13:38.743" // it rounds to .000, .003, or .007
        val expectedMidDateTimeNoMs = "2020-11-09 11:13:38.0"
        val expectedMaxDateTime = "9999-12-31 23:59:59.997"

        ingestValuesWithExpected(DATETIME,
          minDateTime, midDateTime, midDateTimeNoMs, maxDateTime)(
          expectedMinDateTime, expectedMidDateTime, expectedMidDateTimeNoMs, expectedMaxDateTime)
      }

      "datetime2" >> {
        val minDateTime = "0001-01-01T00:00:00.000000"
        val midDateTime = "2020-11-09T11:13:38.742128"
        val midDateTimeNoMs = "2020-11-09T11:13:38"
        val maxDateTime = "9999-12-31T23:59:59.999999"

        val expectedMinDateTime = "0001-01-01 00:00:00.000000"
        val expectedMidDateTime = "2020-11-09 11:13:38.742128"
        val expectedMidDateTimeNoMs = "2020-11-09 11:13:38.000000"
        val expectedMaxDateTime = "9999-12-31 23:59:59.999999"

        ingestValuesWithExpected(DATETIME2(6),
          minDateTime, midDateTime, midDateTimeNoMs, maxDateTime)(
          expectedMinDateTime, expectedMidDateTime, expectedMidDateTimeNoMs, expectedMaxDateTime)
      }

      "datetimeoffset" >> {
        val minDateTime= "1000-01-01T00:00:00.000000-14:00"
        val midDateTime= "2020-11-09T09:12:43.873004-04:00"
        val midDateTimeNoMs = "2020-11-09T09:12:43-04:00"
        val maxDateTime = "9999-12-31T23:59:59.999999+14:00"

        val expectedMinDateTime= "1000-01-01 00:00:00.000000 -14:00"
        val expectedMidDateTime= "2020-11-09 09:12:43.873004 -04:00"
        val expectedMidDateTimeNoMs = "2020-11-09 09:12:43.000000 -04:00"
        val expectedMaxDateTime = "9999-12-31 23:59:59.999999 +14:00"

        ingestValuesWithExpected(DATETIMEOFFSET(6),
          minDateTime, midDateTime, midDateTimeNoMs, maxDateTime)(
          expectedMinDateTime, expectedMidDateTime, expectedMidDateTimeNoMs, expectedMaxDateTime)
      }

      "smalldatetime" >> {
        val minDateTime = "1900-01-01T00:00:00"
        val midDateTime = "2020-11-09T04:12:08"
        val maxDateTime = "2079-06-06T23:59:00"

        val expectedMinDateTime = "1900-01-01 00:00:00.0"
        val expectedMidDateTime = "2020-11-09 04:12:00.0"
        val expectedMaxDateTime = "2079-06-06 23:59:00.0"

        ingestValuesWithExpected(SMALLDATETIME, minDateTime, midDateTime, maxDateTime)(
          expectedMinDateTime, expectedMidDateTime, expectedMaxDateTime)
      }
    }
    /*

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
    */

    "multiple fields with double-quoted string" >> {
      val row1 = "2020-07-12,123456.234234,\"hello world\",887798"
      val row2 = "1983-02-17,732345.987,\"lorum, ipsum\",42"

      val input = csv(row1, row2)

      val cols: NonEmptyList[Column[SQLServerType]] = NonEmptyList.of(
        Column("A", DATE),
        Column("B", DECIMAL(12, 6)),
        Column("C", VARCHAR(20)),
        Column("D", INT))

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
    /*

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
