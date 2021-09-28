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

import quasar.plugin.sqlserver.TestHarness

import scala._, Predef._
import java.time._

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.implicits.javatime._

import fs2.Stream

import org.slf4s.Logging

import quasar.{IdStatus, ScalarStage, ScalarStages}
import quasar.ScalarStages
import quasar.api.{ColumnType, DataPathSegment}
import quasar.api.push.{InternalKey, ExternalOffsetKey}
import quasar.api.resource.ResourcePath
import quasar.common.{CPath, CPathField}
import quasar.common.data.RValue
import quasar.connector.{QueryResult, Offset}
import quasar.connector.datasource.DatasourceModule
import quasar.lib.jdbc.JdbcDiscovery
import quasar.qscript.InterpretedRead

import skolems.∃

object SQLServerDatasourceSpec extends TestHarness with Logging {
  import RValue._

  sequential // FIXME why don't these run in parallel

  type DS = DatasourceModule.DS[IO]

  def harnessed(jdbcUrl: String = TestUrl(Some(TestDb)))
      : Resource[IO, (Transactor[IO], DS, ResourcePath, String)] =
    tableHarness(jdbcUrl, true) map {
      case (xa, path, name) =>
        val disc = JdbcDiscovery(SQLServerDatasourceModule.discoverableTableTypes(log))
        (xa, SQLServerDatasource(xa, disc, log), path, name)
    }

  def loadRValues(ds: DS, p: ResourcePath, stages: ScalarStages = ScalarStages.Id): IO[List[RValue]] =
    ds.loadFull(InterpretedRead(p, stages)).value use {
      case Some(QueryResult.Parsed(_, data, _)) =>
        data.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)

      case _ => IO.pure(List[RValue]())
    }

  def seekRValues(ds: DS, p: ResourcePath, offset: Offset): IO[List[RValue]] =
    ds.loadFrom(InterpretedRead(p, ScalarStages.Id), offset).value use {
      case Some(QueryResult.Parsed(_, data, _)) =>
        data.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)
      case _ => IO.pure(List[RValue]())
    }

  def obj(assocs: (String, RValue)*): RValue =
    rObject(Map(assocs: _*))

  "loading data" >> {
    def obj(assocs: (String, RValue)*): RValue =
      rObject(Map(assocs: _*))

    "boolean" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          x <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (b BIT)").update.run
          y <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (b) VALUES (0), (1)").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(rBoolean(true), rBoolean(false)).map(b => obj("b" -> b))
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "string" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (c CHAR(5), vc VARCHAR(5), txt TEXT)").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (c, vc, txt) VALUES ('abcde', 'fghij', 'klmnopqrs'), ('foo', 'bar', 'baz')").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj("c" -> rString("abcde"), "vc" -> rString("fghij"), "txt" -> rString("klmnopqrs")),
            obj("c" -> rString("foo  "), "vc" -> rString("bar"), "txt" -> rString("baz")))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    // SQL_VARIANT
    "sqlvariant" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (v SQL_VARIANT)").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0""" (v) VALUES ('{"foo":42}'), ('{"foo":-42}')""").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj("v" -> rString("{\"foo\":42}")),
            obj("v" -> rString("{\"foo\":-42}")))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    // TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DECIMAL | NUMERIC
    // TINYINT ranges [0, 255]
    "number" >> {
      def insert(tbl: String, tiny: Int, small: Int, norm: Int, big: Long, flt: Double, dec: BigDecimal, num: BigDecimal): ConnectionIO[Int] = {
        val sql =
          fr"INSERT INTO" ++ frag(tbl) ++ fr0" (tiny, small, norm, big, flt, xct, num) values ($tiny, $small, $norm, $big, $flt, $dec, $num)"

        sql.update.run
      }

      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (tiny TINYINT, small SMALLINT, norm INT, big BIGINT, flt FLOAT, xct DECIMAL(38, 10), num NUMERIC(38, 2))").update.run

          _ <- insert(name,
            0,
            -32768,
            -2147483648,
            -9223372036854775808L,
            -1.7976931348623157E+308,
            BigDecimal("-9999999999999999999999999999.9999999999"),
            BigDecimal("-999999999999999999999999999999999999.99"))

          _ <- insert(name,
            255,
            32767,
            2147483647,
            9223372036854775807L,
            1.7976931348623157E+308,
            BigDecimal("9999999999999999999999999999.9999999999"),
            BigDecimal("999999999999999999999999999999999999.99"))
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj(
              "tiny" -> rLong(0L),
              "small" -> rLong(-32768L),
              "norm" -> rLong(-2147483648L),
              "big" -> rLong(-9223372036854775808L),
              "flt" -> rDouble(-1.7976931348623157E+308),
              "xct" -> rNum(BigDecimal("-9999999999999999999999999999.9999999999")),
              "num" -> rNum(BigDecimal("-999999999999999999999999999999999999.99"))),
            obj(
              "tiny" -> rLong(255L),
              "small" -> rLong(32767L),
              "norm" -> rLong(2147483647L),
              "big" -> rLong(9223372036854775807L),
              "flt" -> rDouble(1.7976931348623157E+308),
              "xct" -> rNum(BigDecimal("9999999999999999999999999999.9999999999")),
              "num" -> rNum(BigDecimal("999999999999999999999999999999999999.99"))))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    // TIME | DATE | DATETIME | DATETIME2 | DATETIMEOFFSET | SMALLDATETIME
    "temporal" >> {
      val minDate = LocalDate.parse("0001-01-01")
      val maxDate = LocalDate.parse("9999-12-31")

      val minTime = LocalTime.parse("00:00:00.000000")
      val maxTime = LocalTime.parse("23:59:59.000000")

      val minDateTime = LocalDateTime.parse("1753-01-01T00:00:00.000")
      val maxDateTime = LocalDateTime.parse("9999-12-31T23:59:59.997")

      val minDateTime2 = LocalDateTime.parse("0001-01-01T00:00:00.000000")
      val maxDateTime2 = LocalDateTime.parse("9999-12-31T23:59:59.999999")

      val minDateTimeOffset = OffsetDateTime.parse("1000-01-01T00:00:00.000000-14:00")
      val maxDateTimeOffset = OffsetDateTime.parse("9999-12-31T23:59:59.999999+14:00")

      val minSmallDateTime = LocalDateTime.parse("1900-01-01T00:00:00")
      val maxSmallDateTime = LocalDateTime.parse("2079-06-06T23:59:00")

      def insert(tbl: String, lt: LocalTime, ld: LocalDate, dt: LocalDateTime, dt2: LocalDateTime, dto: OffsetDateTime, sdt: LocalDateTime): ConnectionIO[Int] = {
        val sql =
          fr"INSERT INTO" ++ frag(tbl) ++ fr0" (lt, ld, dt, dt2, dto, sdt) values ($lt, $ld, $dt, $dt2, $dto, $sdt)"

        sql.update.run
      }

      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (lt TIME(6), ld DATE, dt DATETIME, dt2 DATETIME2(6), dto DATETIMEOFFSET(6), sdt SMALLDATETIME)").update.run

          _ <- insert(name, minTime, minDate, minDateTime, minDateTime2, minDateTimeOffset, minSmallDateTime)
          _ <- insert(name, maxTime, maxDate, maxDateTime, maxDateTime2, maxDateTimeOffset, maxSmallDateTime)
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj(
              "lt" -> rLocalTime(minTime),
              "ld" -> rLocalDate(minDate),
              "dt" -> rLocalDateTime(minDateTime),
              "dt2" -> rLocalDateTime(minDateTime2),
              "dto" -> rOffsetDateTime(minDateTimeOffset),
              "sdt" -> rLocalDateTime(minSmallDateTime)),
            obj(
              "lt" -> rLocalTime(maxTime),
              "ld" -> rLocalDate(maxDate),
              "dt" -> rLocalDateTime(maxDateTime),
              "dt2" -> rLocalDateTime(maxDateTime2),
              "dto" -> rOffsetDateTime(maxDateTimeOffset),
              "sdt" -> rLocalDateTime(maxSmallDateTime)))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    "empty table returns empty results" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (ts TIME(6), n INT, f FLOAT, x DATE)").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          results must beEmpty
        }
      }
    }
  }

  "seek data" >> {
    import DataPathSegment._

    "errors when path is incorrect" >> {
      val key = ∃(InternalKey.Actual.string("1"))
      val twoFields = Offset.Internal(NonEmptyList.of(Field("foo"), Field("bar")), key)
      val index = Offset.Internal(NonEmptyList.one(Index(0)), key)
      val all = Offset.Internal(NonEmptyList.one(AllFields), key)
      val allIndices = Offset.Internal(NonEmptyList.one(AllIndices), key)
      val external = Offset.External(ExternalOffsetKey(Array(0x01, 0x01)))

      harnessed() use { case (xa, ds, path, name) =>
        for {
          _ <- (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (col INTEGER)").update.run.transact(xa)
          two <- seekRValues(ds, path, twoFields).attempt
          ind <- seekRValues(ds, path, index).attempt
          allF <- seekRValues(ds, path, all).attempt
          allI <- seekRValues(ds, path, allIndices).attempt
          ext <- seekRValues(ds, path, external).attempt
        } yield {
          two must beLeft
          ind must beLeft
          allF must beLeft
          allI must beLeft
          ext must beLeft
        }
      }
    }

    "string offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset VARCHAR(32))").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (0, 'foo')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, 'bar')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, 'baz')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("OFFSET")), ∃(InternalKey.Actual.string("baz")))

        val expected = List(
          obj("id" -> rLong(2L), "offset" -> rString("baz")),
          obj("id" -> rLong(0L), "offset" -> rString("foo")))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "numeric offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset INTEGER)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (0, 1)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, 2)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, 3)").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("OFFSET")), ∃(InternalKey.Actual.real(2)))

        val expected = List(
          obj("id" -> rLong(1L), "offset" -> rLong(2L)),
          obj("id" -> rLong(2L), "offset" -> rLong(3L)))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "dateTime offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset datetime)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (0, '1800-01-01T01:01:01')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, '2000-02-02T02:02:02')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, '3000-03-03T03:03:03')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("OFFSET")), ∃(InternalKey.Actual.dateTime(
            OffsetDateTime.parse(s"2000-02-02T02:02:02+00:00"))))

        val expected = List(
          obj(
            "id" -> rLong(1L),
            "offset" -> rLocalDateTime(LocalDateTime.parse("2000-02-02T02:02:02"))),
          obj(
            "id" -> rLong(2L),
            "offset" -> rLocalDateTime(LocalDateTime.parse("3000-03-03T03:03:03"))))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "localDateTime offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset datetime2)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (0, '1000-01-01T01:01:01')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, '2000-02-02T02:02:02')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, '3000-03-03T03:03:03')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("OFFSET")), ∃(InternalKey.Actual.localDateTime(
            LocalDateTime.parse(s"2000-02-02T02:02:02"))))

        val expected = List(
          obj(
            "id" -> rLong(1L),
            "offset" -> rLocalDateTime(LocalDateTime.parse("2000-02-02T02:02:02"))),
          obj(
            "id" -> rLong(2L),
            "offset" -> rLocalDateTime(LocalDateTime.parse("3000-03-03T03:03:03"))))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "localDate offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset date)").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (0, '1000-01-01')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, '2000-02-02')").update.run >>
            (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, '3000-03-03')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("OFFSET")), ∃(InternalKey.Actual.localDate(
            LocalDate.parse(s"2000-02-02"))))

        val expected = List(
          obj(
            "id" -> rLong(1L),
            "offset" -> rLocalDate(LocalDate.parse("2000-02-02"))),
          obj(
            "id" -> rLong(2L),
            "offset" -> rLocalDate(LocalDate.parse("3000-03-03"))))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
  }

  "masking doesn't elide timestamps" >> {
    harnessed() use { case (xa, ds, path, name) =>
      val setup =
        (fr"CREATE TABLE" ++ Fragment.const0(name) ++ fr0" (id INTEGER, offset DATETIMEOFFSET)")
          .update.run >>
        (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (1, '2000-02-02 02:02:02 +00:00')")
          .update.run >>
        (fr"INSERT INTO" ++ Fragment.const0(name) ++ fr0" VALUES (2, '3000-03-03 03:03:03 +00:00')")
          .update.run

      val expected = List(
        obj(
          "offset" -> rOffsetDateTime(OffsetDateTime.parse("2000-02-02T02:02:02+00:00"))),
        obj(
          "offset" -> rOffsetDateTime(OffsetDateTime.parse("3000-03-03T03:03:03+00:00"))))

      val stages = ScalarStages(IdStatus.ExcludeId, List(ScalarStage.Mask(Map(
        CPath(CPathField("offset")) -> Set(ColumnType.OffsetDateTime)))))

      setup.transact(xa) >> loadRValues(ds, path, stages) map { results =>
        results must containTheSameElementsAs(expected)
      }
    }
  }
}
