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

import quasar.plugin.sqlserver.TestHarness

import scala.{text => _, Stream => _, _}, Predef._

import java.lang.CharSequence
import java.time._
import scala.util.Random

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

  sequential // FIXME why don't these run in parallel

  def createSink(
      dest: SQLServerDestination[IO],
      path: ResourcePath,
      cols: NonEmptyList[Column[SQLServerType]])
      : Pipe[IO, CharSequence, Unit] =
    dest.createSink(path, cols)._2

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

  def quote(chars: String): String = s"'$chars'"

  def delim(lines: String*): Stream[IO, CharSequence] =
    Stream.emits(lines)
      //.intersperse("\r\n")
      //.through(text.utf8Encode) // FIXME utf8encode?

  def ingestValues[A: Read](tpe: SQLServerType, values: A*) =
    ingestValuesWithExpected(tpe, values: _*)(values: _*)

  def ingestValuesWithExpected[A: Read](tpe: SQLServerType, values: A*)(expected: A*) = {
    val input = delim(values.map(_.toString): _*)
    val cols = NonEmptyList.one(Column("value", tpe))

    harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
      for {
        _ <- input.through(createSink(dest, path, cols)).compile.drain
        vals <- frag(s"select value from $tableName").query[A].to[List].transact(xa)
      } yield {
        vals must containTheSameElementsAs(expected)
      }
    }
  }

  "write mode" >> {
    val cols = NonEmptyList.one(Column("value", CHAR(1)))

    "create" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "fails when table present" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beLeft
          }
        }
      }
    }

    "replace" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "truncate" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "append" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }
  }

  "ingest" >> {
    "boolean" >> {
      val input = delim("1", "0")
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
    }

    "string" >> {
      // TEXT doesn't support Unicode
      "text" >> ingestValuesWithExpected(TEXT, "'føobår'", "' b a t'")("føobår", " b a t")

      //TODO Unicode: COLLATE Latin1_General_100_CI_AI_SC_UTF8
      "char" >> ingestValuesWithExpected(CHAR(6), "'føobår'", "' b a t'")("føobår", " b a t")

      //TODO Unicode: COLLATE Latin1_General_100_CI_AI_SC_UTF8
      "varchar" >> ingestValuesWithExpected(VARCHAR(6), "'føobår'", "' b a t'")("føobår", " b a t")

      //TODO Unicode: COLLATE Latin1_General_100_CI_AI_SC_UTF8
      "ntext" >> ingestValuesWithExpected(NTEXT, "'føobår'", "' b a t'")("føobår", " b a t")

      "nchar" >> ingestValuesWithExpected(NCHAR(6), "'føobår'", "N'  ひらがな'", "' b a t'")("føobår", "  ひらがな", " b a t")

      "nvarchar" >> ingestValuesWithExpected(NVARCHAR(6), "'føobår'", "N'  ひらがな'", "' b a t'")("føobår", "  ひらがな", " b a t")
    }

    "temporal" >> {
      "date" >> {
        val minDate = "0001-01-01"
        val midDate = "2020-11-09"
        val maxDate = "9999-12-31"

        ingestValuesWithExpected(DATE, quote(minDate), quote(midDate), quote(maxDate))(
          minDate, midDate, maxDate)
      }

      "time" >> {
        val minTime = "00:00:00.000000"
        val midTime = "11:13:52.738493"
        val midTimeNoMs = "11:13:52"
        val maxTime = "23:59:59.000000"

        ingestValuesWithExpected(TIME(6), quote(minTime), quote(midTime), quote(midTimeNoMs), quote(maxTime))(
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
          quote(minDateTime), quote(midDateTime), quote(midDateTimeNoMs), quote(maxDateTime))(
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
          quote(minDateTime), quote(midDateTime), quote(midDateTimeNoMs), quote(maxDateTime))(
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
          quote(minDateTime), quote(midDateTime), quote(midDateTimeNoMs), quote(maxDateTime))(
          expectedMinDateTime, expectedMidDateTime, expectedMidDateTimeNoMs, expectedMaxDateTime)
      }

      "smalldatetime" >> {
        val minDateTime = "1900-01-01T00:00:00"
        val midDateTime = "2020-11-09T04:12:08"
        val maxDateTime = "2079-06-06T23:59:00"

        val expectedMinDateTime = "1900-01-01 00:00:00.0"
        val expectedMidDateTime = "2020-11-09 04:12:00.0"
        val expectedMaxDateTime = "2079-06-06 23:59:00.0"

        ingestValuesWithExpected(SMALLDATETIME, quote(minDateTime), quote(midDateTime), quote(maxDateTime))(
          expectedMinDateTime, expectedMidDateTime, expectedMaxDateTime)
      }
    }

    "containing special characters" >> {
      val escA = "'foo,\",,\"\"'"
      val escB = "'java\"script\"'"
      val escC = "'thisis''stuff'"

      val A = "foo,\",,\"\""
      val B = "java\"script\""
      val C = "thisis'stuff"

      val input = delim(escA, escB, escC)
      val cols = NonEmptyList.one(Column("value", VARCHAR(12)))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select value from $tableName")
              .query[String].to[List].transact(xa)
        } yield {
          vals must contain(A, B, C)
        }
      }
    }

    "multiple fields with double-quoted string" >> {
      val row1 = "'2020-07-12',123456.234234,'hello world',887798"
      val row2 = "'1983-02-17',732345.987,'lorum, ipsum',42"

      val input = delim(row1, row2)

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

    "undefined fields" >> {
      val row1 = s"NULL,42,'1992-03-04'"
      val row2 = s"'foo',NULL,'1992-03-04'"
      val row3 = s"'foo',42,NULL"

      val input = delim(row1, row2, row3)

      val cols = NonEmptyList.of(
        Column("A", CHAR(3)),
        Column("B", TINYINT),
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

    "user defined schema" >> {
      val row1 = s"'foo',42,'1992-03-04'"

      val input = delim(row1)

      val cols = NonEmptyList.of(
        Column("A", CHAR(3)),
        Column("B", TINYINT),
        Column("C", DATE))

      // bulkCopy.setDestinationTableName cannot handle a number as the first char of a schema name
      val testSchema = IO("a" ++ Random.alphanumeric.take(5).mkString)

      testSchema.flatMap(sch => harnessed(schema = sch) use { case (xa, dest, path, tableName) =>
        for {
          _ <- frag(s"DROP SCHEMA IF EXISTS [$sch]").update.run.transact(xa)
          _ <- frag(s"CREATE SCHEMA [$sch]").update.run.transact(xa)

          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select A, B, C from [$sch].[$tableName]")
              .query[(Option[String], Option[Int], Option[LocalDate])]
              .to[List].transact(xa)
        } yield {
          vals must contain(
            (Some("foo"), Some(42), Some(LocalDate.parse("1992-03-04"))))
        }
      })
    }

    "Errors when table does not exist" >> {
      val row1 = "'foo',42,'1992-03-04'"

      val input = delim(row1)

      val cols = NonEmptyList.of(
        Column("A", CHAR(3)),
        Column("B", TINYINT),
        Column("C", DATE))

      // bulkCopy.setDestinationTableName cannot handle a number as the first char of a schema name
      val testSchema = IO("a" ++ Random.alphanumeric.take(5).mkString)

      testSchema.flatMap(sch => harnessed(schema = sch) use { case (xa, dest, path, tableName) =>
        for {
          _ <- frag(s"DROP SCHEMA IF EXISTS [$sch]").update.run.transact(xa)
          _ <- frag(s"CREATE SCHEMA [$sch]").update.run.transact(xa)

          _ <- input.through(createSink(dest, path, cols)).compile.drain

          attempted <-
            frag(s"select A, B, C from [nottestschema].[$tableName]")
              .query[(Option[String], Option[Int], Option[LocalDate])]
              .to[List].transact(xa).attempt
        } yield {
          attempted must beLike {
            case Left(throwable) =>
              throwable.getMessage.take(19) mustEqual("Invalid object name")
          }
        }
      })
    }
  }
}
