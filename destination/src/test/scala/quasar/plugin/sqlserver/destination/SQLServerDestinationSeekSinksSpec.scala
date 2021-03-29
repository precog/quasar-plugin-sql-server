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

import slamdata.Predef._

import quasar.plugin.sqlserver._

import quasar.EffectfulQSpec
import quasar.api.Column
import quasar.api.push.{OffsetKey, PushColumns}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector._
import quasar.connector.destination.{WriteMode => QWriteMode, _}
import quasar.contrib.scalaz.MonadError_
import quasar.lib.jdbc.{ColumnName, Ident, JdbcDiscovery, TableName}

import argonaut._, Argonaut._, ArgonautScalaz._

import cats.Eq
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.Read

import java.lang.CharSequence
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import fs2.{Chunk, Pull, Stream}

import org.specs2.execute.AsResult
import org.specs2.specification.BeforeAll
import org.specs2.specification.core.{Fragment => SFragment}

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._
import shapeless.syntax.singleton._

import scalaz.syntax.show._

object SQLServerDestinationSeekSinksSpec extends EffectfulQSpec[IO] with BeforeAll {
  sequential

  val harness = new TestHarness {}

  def beforeAll(): Unit =
   harness.beforeAll()

  def config(schema: Option[String] = None): Json = {
    val connectionJson =
      ("jdbcUrl" := harness.TestUrl(harness.TestDb.some)) ->:
      ("parameters" := List[Json]()) ->:
      ("maxConcurrentcy" := jNull) ->:
      ("maxLifetime" := jNull) ->:
      jEmptyObject
    ("connection" := connectionJson) ->:
    ("schema" := schema) ->:
    ("writeMode" := "replace") ->:
    jEmptyObject
  }

  val Mod = SQLServerDestinationModule

  "seek sinks (upsert and append)" should {
    val XStringCol = Column("x", SQLServerType.TEXT)
    "write after commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "quux") :: HNil)),
          UpsertEvent.Commit("commit1"))
      for {
        table <- freshTableName
        (values, offsets) <- consumer(table, toOpt(XStringCol), QWriteMode.Replace, events)
      } yield {
        values must_== List(
          "foo" :: "bar" :: HNil,
          "baz" :: "quux" :: HNil)
      }
    }
    "write two chunks with a single commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "not write without a commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(("x" ->> "quz") :: ("y" ->> "corge") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Create(List(("x" ->> "baz") :: ("y" ->> "qux") :: HNil)))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "quz" :: "corge" :: HNil)
          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "commit twice in a row" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List("foo" :: "bar" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
    }

    "upsert delete rows with string typed primary key" >>* {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List("baz" :: "qux" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert delete rows with long typed primary key" >>* {
      Consumer.upsert[Int :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "bar") :: HNil,
                ("x" ->> 42) :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.LongIds(List(40))),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", SQLServerType.INT)), QWriteMode.Replace, events)
        } yield {
          values must_== List(42 :: "qux" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert empty deletes without failing" >>* {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List())),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert delete same id twice" >>* {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit2"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit3"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must_== List("baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"),
            OffsetKey.Actual.string("commit3"))
        }
      }
    }

    "creates table and then appends" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events1 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"))

      val events2 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "bar") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName

        _ <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events1)

        (values, _) <- consumer(tbl, Some(XStringCol), QWriteMode.Append, events2)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "bar" :: "qux" :: HNil)
        }
    }

    "creates an index for each table on correlation id" >> Consumer.idOnly[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tblA <- freshTableName
        tblB <- freshTableName

        _ <- consumer(tblA, toOpt(XStringCol), QWriteMode.Replace, events)

        _ <- consumer(tblB, toOpt(XStringCol), QWriteMode.Replace, events)

        objectIds = List(tblA, tblB).map(x => Fragment.const0(s"OBJECT_ID('dbo.$x')")).intercalate(fr0",")

        names = NonEmptyList.of(tblA, tblB).map(indexName)

        checkIndexes =
          fr"SELECT count(*) FROM sys.indexes WHERE" ++
            Fragments.in(fr"name", names) ++
            fr"AND object_id IN" ++
            Fragments.parentheses(objectIds)

        indexCount <- runDb[Int](checkIndexes.query[Int].unique)
      } yield {
        indexCount must_=== 2
      }
    }

    "ensures id column with 'default' unindexable type converted to indexable" >> Consumer.idOnly[String :: String :: HNil] { (toOpt, consumer) =>

      import JdbcDiscovery.ColumnMeta

      val discovery = JdbcDiscovery(None)

      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tblA <- freshTableName
        _ <- consumer(tblA, toOpt(XStringCol), QWriteMode.Replace, events)

        indexedColumn =
          discovery.tableColumns(TableName(tblA), None)
            .filter(_.name === ColumnName("x"))
            .compile
            .lastOrError

        columnMeta <- runDb[ColumnMeta](indexedColumn)
      } yield {
        columnMeta.vendorType must_=== "varchar"
      }
    }

    "doesn't create indices if there is no ids" >>*
      Consumer.append[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(List(
              ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
              ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

        for {
          tblA <- freshTableName
          tblB <- freshTableName

          _ <- consumer(tblA, None, QWriteMode.Replace, events)

          _ <- consumer(tblB, None, QWriteMode.Replace, events)

          objectIds = List(tblA, tblB).map(x => Fragment.const0(s"OBJECT_ID('dbo.$x')")).intercalate(fr0",")

          names = NonEmptyList.of(tblA, tblB).map(indexName)

          checkIndexes =
            fr"SELECT count(*) FROM sys.indexes WHERE" ++
              Fragments.in(fr"name", names) ++
              fr"AND object_id IN" ++
              Fragments.parentheses(objectIds)

          indexCount <- runDb[Int](checkIndexes.query[Int].unique)
        } yield {
          indexCount must_=== 0
        }
      }
  }

  implicit val CS: ContextShift[IO] = IO.contextShift(global)

  implicit val TM: Timer[IO] = IO.timer(global)

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val freshTableName: IO[String] =
    IO(s"mssqltest${Random.alphanumeric.take(6).mkString}")

  private def dest(cfg: Json): Resource[IO, Destination[IO]] =
    Mod.destination[IO](cfg, _ => _ => Stream.empty) flatMap {
      case Left(err) => Resource.liftF(IO.raiseError(new RuntimeException(err.shows)))
      case Right(d) => d.pure[Resource[IO, *]]
    }

  trait Consumer[V <: HList]{
    def apply[R <: HList, K <: HList, T <: HList, S <: HList](
        table: String,
        idColumn: Option[Column[SQLServerType]],
        writeMode: QWriteMode,
        records: Stream[IO, UpsertEvent[R]])(
        implicit
        read: Read[V],
        keys: Keys.Aux[R, K],
        values: Values.Aux[R, V],
        getTypes: Mapper.Aux[columnType.type, V, T],
        rrow: Mapper.Aux[render.type, V, S],
        ktl: ToList[K, String],
        vtl: ToList[S, String],
        ttl: ToList[T, SQLServerType])
        : IO[(List[V], List[OffsetKey.Actual[String]])]
  }

  object Consumer {
    def upsert[V <: HList](cfg: Json = config()): Resource[IO, Consumer[V]] = {
      val rsink: Resource[IO, ResultSink.UpsertSink[IO, SQLServerType, CharSequence]] =
        dest(cfg) evalMap { dst =>
          val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.UpsertSink(_) => c }
          optSink match {
            case Some(s) => s.asInstanceOf[ResultSink.UpsertSink[IO, SQLServerType, CharSequence]].pure[IO]
            case None => IO.raiseError(new RuntimeException("No upsert sink found"))
          }
        }
      rsink map { sink => new Consumer[V] {
        def apply[R <: HList, K <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[SQLServerType]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[V],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[columnType.type, V, T],
            rrow: Mapper.Aux[render.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, SQLServerType])
            : IO[(List[V], List[OffsetKey.Actual[String]])] = for {
          columns <- columnsOf(records, idColumn).compile.lastOrError
          colList = hygienicColumns(idColumn.get :: columns).map((x: (HI, SQLServerType)) =>
            Fragment.const0(x._1.forSqlName)).intercalate(fr",")
          dst = ResourcePath.root() / ResourceName(table)
          offsets <- toUpsertSink(sink, dst, idColumn.get, writeMode, records).compile.toList
          q = fr"SELECT" ++
            colList ++
            fr"FROM" ++
            Fragment.const(SQLServerHygiene.hygienicIdent(Ident(table)).forSqlName)
          rows <- runDb[List[V]](q.query[V].to[List])
        } yield (rows, offsets)
      }}
    }

    def append[V <: HList](cfg: Json = config()): Resource[IO, Consumer[V]] = {
      val rsink: Resource[IO, ResultSink.AppendSink[IO, SQLServerType]] =
        dest(cfg) evalMap { dst =>
          val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.AppendSink(_) => c }
          optSink match {
            case Some(s) => s.asInstanceOf[ResultSink.AppendSink[IO, SQLServerType]].pure[IO]
            case None => IO.raiseError(new RuntimeException("No append sink found"))
          }
        }

      rsink map { sink => new Consumer[V] {
        def apply[R <: HList, K <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[SQLServerType]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[V],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[columnType.type, V, T],
            rrow: Mapper.Aux[render.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, SQLServerType])
            : IO[(List[V], List[OffsetKey.Actual[String]])] = for {
          tableColumns <- columnsOf(records, idColumn).compile.lastOrError
          colList = hygienicColumns(idColumn.toList ++ tableColumns).map((x: (HI, SQLServerType)) =>
            Fragment.const0(x._1.forSqlName)).intercalate(fr",")
          dst = ResourcePath.root() / ResourceName(table)
          offsets <- toAppendSink(sink, dst, idColumn, writeMode, records).compile.toList
          q = fr"SELECT" ++
            colList ++
            fr"FROM" ++
            Fragment.const(SQLServerHygiene.hygienicIdent(Ident(table)).forSqlName)
          rows <- runDb[List[V]](q.query[V].to[List])
        } yield (rows, offsets)
      }}
    }

    type MkOption = Column[SQLServerType] => Option[Column[SQLServerType]]

    object appendAndUpsert {
      def apply[A <: HList]: PartiallyApplied[A] = new PartiallyApplied[A]

      final class PartiallyApplied[A <: HList] {
        def apply[R: AsResult](f: (MkOption, Consumer[A]) => IO[R]): SFragment = {
          "upsert" >>* Consumer.upsert[A]().use(f(Some(_), _))
          "append" >>* Consumer.append[A]().use(f(Some(_), _))
          "no-id" >>* Consumer.append[A]().use(f(x => None, _))
        }
      }
    }

    object idOnly {
      def apply[A <: HList]: PartiallyApplied[A] = new PartiallyApplied[A]

      final class PartiallyApplied[A <: HList] {
        def apply[R: AsResult](f: (MkOption, Consumer[A]) => IO[R]): SFragment = {
          "upsert" >>* Consumer.upsert[A]().use(f(Some(_), _))
          "append" >>* Consumer.append[A]().use(f(Some(_), _))
        }
      }
    }
  }

  def runDb[A](fa: ConnectionIO[A]): IO[A] =
    harness.TestXa(harness.TestUrl(harness.TestDb.some)).use(xa => fa.transact(xa))

  def columnsOf[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList](
      events: Stream[F, UpsertEvent[R]],
      idColumn: Option[Column[SQLServerType]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      ktl: ToList[K, String],
      ttl: ToList[T, SQLServerType])
      : Stream[F, List[Column[SQLServerType]]] = {
    val go = events.pull.peek1 flatMap {
      case Some((UpsertEvent.Create(records), _)) => records.headOption match {
        case Some(r) =>
          val rkeys = r.keys.toList
          val rtypes = r.values.map(columnType).toList
          val columns = rkeys.zip(rtypes).map((Column[SQLServerType] _).tupled)
          Pull.output1(columns.filter(c => c.some =!= idColumn))
        case _ =>
          Pull.done
      }
      case _ => Pull.done
    }
    go.stream
  }

  def toAppendSink[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      sink: ResultSink.AppendSink[F, SQLServerType],
      dst: ResourcePath,
      idColumn: Option[Column[SQLServerType]],
      writeMode: QWriteMode,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      renderValues: Mapper.Aux[render.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, SQLServerType])
      : Stream[F, OffsetKey.Actual[String]] = {

    val appends = appendEvents[F, R, V, S](events)

    type Consumed = ResultSink.AppendSink.Result[F] { type A = CharSequence }
    def toConsumed(a: ResultSink.AppendSink.Result[F]): Consumed = a.asInstanceOf[Consumed]

    for {
      colList <- columnsOf(events, idColumn)
      cols = idColumn match {
        case None => PushColumns.NoPrimary(NonEmptyList.fromListUnsafe(colList))
        case Some(i) => PushColumns.HasPrimary(List(), i, colList)
      }
      consumed = sink.consume.apply(
        ResultSink.AppendSink.Args(dst, cols, writeMode))
      res <- appends.through(toConsumed(consumed).pipe[String])
    } yield res
  }

  def appendEvents[F[_]: Async, R <: HList, V <: HList, S <: HList](
      events: Stream[F, UpsertEvent[R]])(
      implicit
      values: Values.Aux[R, V],
      renderValues: Mapper.Aux[render.type, V, S],
      stl: ToList[S, String])
      : Stream[F, AppendEvent[CharSequence, OffsetKey.Actual[String]]] = events flatMap {
    case UpsertEvent.Commit(s) =>
      Stream(DataEvent.Commit(OffsetKey.Actual.string(s)))
    case UpsertEvent.Delete(_) =>
      Stream.eval_(Async[F].raiseError(new RuntimeException("AppendSink can't handle delete events")))
    case UpsertEvent.Create(records) =>
      Stream.emits(records)
        .covary[F]
        .map(r => Chunk(r.values.map(render).toList.mkString(",")))
        .map(DataEvent.Create(_))
  }

  def toUpsertSink[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      sink: ResultSink.UpsertSink[F, SQLServerType, CharSequence],
      dst: ResourcePath,
      idColumn: Column[SQLServerType],
      writeMode: QWriteMode,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      renderValues: Mapper.Aux[render.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, SQLServerType])
      : Stream[F, OffsetKey.Actual[String]] = {
    val upserts = dataEvents[F, R, V, S](events)

    columnsOf(events, Some(idColumn)) flatMap { cols =>
      val (_, pipe) = sink.consume.apply(
        ResultSink.UpsertSink.Args(dst, idColumn, cols, writeMode))
      upserts.through(pipe[String])
    }
  }

  def dataEvents[F[_]: Async, R <: HList, V <: HList, S <: HList](
      events: Stream[F, UpsertEvent[R]])(
      implicit
      values: Values.Aux[R, V],
      renderValues: Mapper.Aux[render.type, V, S],
      stl: ToList[S, String])
      : Stream[F, DataEvent[CharSequence, OffsetKey.Actual[String]]] = events flatMap {
    case UpsertEvent.Commit(s) =>
      Stream(DataEvent.Commit(OffsetKey.Actual.string(s)))
    case UpsertEvent.Delete(ids) => ids match {
      case Ids.StringIds(is) =>
        Stream(DataEvent.Delete(IdBatch.Strings(is.toArray, is.length)))
      case Ids.LongIds(is) =>
        Stream(DataEvent.Delete(IdBatch.Longs(is.toArray, is.length)))
    }
    case UpsertEvent.Create(records) =>
      Stream.emits(records).covary[F] map { r =>
        DataEvent.Create(Chunk(r.values.map(render).toList.mkString(",")))
      }
  }

  sealed trait UpsertEvent[+A] extends Product with Serializable
  sealed trait Ids extends Product with Serializable

  object UpsertEvent {
    case class Create[A](records: List[A]) extends UpsertEvent[A]
    case class Delete(recordIds: Ids) extends UpsertEvent[Nothing]
    case class Commit(value: String) extends UpsertEvent[Nothing]
  }

  object Ids {
    case class StringIds(ids: List[String]) extends Ids
    case class LongIds(ids: List[Long]) extends Ids
  }

  object columnType extends Poly1 {
    import SQLServerType._
    // Set to the 'preferred/default' coercion for 'ColumnType.String'
    // to test indexability adjustments are applied.
    implicit val stringCase: Case.Aux[String, SQLServerType] = at(_ => TEXT)
    implicit val intCase: Case.Aux[Int, SQLServerType] = at(_ => INT)
  }

  object render extends Poly1 {
    implicit val stringCase: Case.Aux[String, String] = at(s => s"'$s'")
    implicit val intCase: Case.Aux[Int, String] = at(_.toString)
  }

  // This is needed only for testing purpose
  implicit val eqSQLServerType: Eq[SQLServerType] = Eq.fromUniversalEquals
}
