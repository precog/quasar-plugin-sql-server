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

import scala.{text => _, Stream => _, _}, Predef._
import scala.concurrent.ExecutionContext
import scala.util.Random

import java.util.concurrent.Executors

import cats.effect.{Blocker, IO, Resource}
import cats.effect.testing.specs2.CatsIO
import cats.implicits._

import doobie._
import doobie.implicits._

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.contrib.scalaz.MonadError_

// sudo docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YourStrong@Passw0rd>" -p 1433:1433 --name sql1 -h sql1 -d mcr.microsoft.com/mssql/server:2019-latest
trait TestHarness extends Specification with CatsIO with BeforeAll {

  implicit val ioMonadResourceErr: MonadResourceErr[IO] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val TestDb: String = "precogtest"

  val frag = Fragment.const0(_, None)

  //val User = "SA"
  //val Password = "<YourStrong@Passw0rd>"
  val User = "sa"
  val Password = "1"

  def TestUrl(db: Option[String]): String =
    s"jdbc:sqlserver://localhost:1433${db.map(";database=" + _).getOrElse("")};user=$User;password=$Password"

  def TestXa(jdbcUrl: String): Resource[IO, Transactor[IO]] =
    Resource.make(IO(Executors.newSingleThreadExecutor()))(p => IO(p.shutdown)) map { ex =>
      Transactor.fromDriverManager(
        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        jdbcUrl,
        Blocker.liftExecutionContext(ExecutionContext.fromExecutorService(ex)))
    }

  def beforeAll(): Unit = ()
   // TestXa(TestUrl(None))
   //   .use((frag(s"IF DB_ID (N'$TestDb') IS NOT NULL DROP DATABASE $TestDb").update.run >>
   //     frag(s"CREATE DATABASE $TestDb").update.run).transact(_))
   //   .attempt
   //   .void
   //   .unsafeRunSync

  def table(xa: Transactor[IO], schemaInPath: Boolean, specialString: String = ""): Resource[IO, (ResourcePath, String)] = {
    val setup = Resource.make(
      IO(s"sqlserver_spec_${Random.alphanumeric.take(6).mkString}" + specialString))(
      name => {
        val n = SQLServerHygiene.elideBrackets(name)
        frag(s"DROP TABLE IF EXISTS $n").update.run.transact(xa).void
      })

    setup map { n =>
      val path = if (schemaInPath)
        ResourcePath.root() / ResourceName("dbo") / ResourceName(n)
      else
        ResourcePath.root() / ResourceName(n)

      (path, n)
    }
  }

  def tableHarness(
      jdbcUrl: String = TestUrl(Some(TestDb)),
      schemaInPath: Boolean,
      specialString: String = "")
      : Resource[IO, (Transactor[IO], ResourcePath, String)] =
    for {
      xa <- TestXa(jdbcUrl)
      (path, name) <- table(xa, schemaInPath, specialString)
    } yield (xa, path, name)
}
