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

import scala._, Predef._

import cats.data.Ior

import doobie.Fragment

import quasar.api.Labeled
import quasar.api.push.param._
import quasar.connector.destination.Constructor

sealed abstract class SQLServerType(spec: String) extends Product with Serializable {
  def asSql: Fragment = Fragment.const0(spec)
}

// https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
object SQLServerType {
  case object BIGINT extends SQLServerTypeId.SelfIdentified("BIGINT", 0)

  case object BINARY extends SQLServerTypeId.SelfIdentified("BINARY", 1)

  final case class CHAR(length: Int) extends SQLServerType(s"CHAR($length)")
  case object CHAR extends SQLServerTypeId.HigherKinded(2) {
    val constructor = Constructor.Unary(LengthCharsParam(8000), CHAR(_))
  }

  case object DATE extends SQLServerTypeId.SelfIdentified("DATE", 3)

  case object DATETIME extends SQLServerTypeId.SelfIdentified("DATETIME", 4)

  final case class DATETIME2(precision: Int) extends SQLServerType(s"DATETIME2($precision)")
  case object DATETIME2 extends SQLServerTypeId.HigherKinded(5) {
    val constructor = Constructor.Unary(PrecisionDateTimeParam(7), DATETIME2(_))
  }

  final case class DATETIMEOFFSET(precision: Int) extends SQLServerType(s"DATETIMEOFFSET($precision)")
  case object DATETIMEOFFSET extends SQLServerTypeId.HigherKinded(6) {
    val constructor = Constructor.Unary(PrecisionDateTimeParam(7), DATETIMEOFFSET(_))
  }

  final case class DECIMAL(precision: Int, scale: Int) extends SQLServerType(s"DECIMAL($precision, $scale)")
  case object DECIMAL extends SQLServerTypeId.HigherKinded(7) {
    val constructor = {
      val precisionParam: Labeled[Formal[Int]] =
        Labeled("Precision", Formal.integer(Some(Ior.both(1, 38)), None, None))

      val scaleParam: Labeled[Formal[Int]] =
        Labeled("Scale", Formal.integer(Some(Ior.both(0, 38)), None, None))

      Constructor.Binary(
        precisionParam,
        scaleParam,
        DECIMAL(_, _))
    }
  }

  final case class FLOAT(precision: Int) extends SQLServerType(s"FLOAT($precision)")
  case object FLOAT extends SQLServerTypeId.HigherKinded(8) {
    val constructor = Constructor.Unary(PrecisionFloatParam(53), FLOAT(_))
  }

  case object INT extends SQLServerTypeId.SelfIdentified("INT", 9)

  case object MONEY extends SQLServerTypeId.SelfIdentified("MONEY", 10)

  final case class NCHAR(length: Int) extends SQLServerType(s"NCHAR($length)")
  case object NCHAR extends SQLServerTypeId.HigherKinded(11) {
    val constructor = Constructor.Unary(LengthCharsParam(4000), NCHAR(_))
  }

  final case class NUMERIC(precision: Int, scale: Int) extends SQLServerType(s"NUMERIC($precision, $scale)")
  case object NUMERIC extends SQLServerTypeId.HigherKinded(12) {
    val constructor = {
      val precisionParam: Labeled[Formal[Int]] =
        Labeled("Precision", Formal.integer(Some(Ior.both(1, 38)), None, None))

      val scaleParam: Labeled[Formal[Int]] =
        Labeled("Scale", Formal.integer(Some(Ior.both(0, 38)), None, None))

      Constructor.Binary(
        precisionParam,
        scaleParam,
        NUMERIC(_, _))
    }
  }

  final case class NVARCHAR(length: Int) extends SQLServerType(s"NVARCHAR($length)")
  case object NVARCHAR extends SQLServerTypeId.HigherKinded(13) {
    val constructor = Constructor.Unary(LengthCharsParam(4000), NVARCHAR(_))
  }

  case object REAL extends SQLServerTypeId.SelfIdentified("REAL", 14)

  case object SMALLDATETIME extends SQLServerTypeId.SelfIdentified("SMALLDATETIME", 15)

  case object SMALLINT extends SQLServerTypeId.SelfIdentified("SMALLINT", 16)

  case object SMALLMONEY extends SQLServerTypeId.SelfIdentified("SMALLMONEY", 17)

  case object TEXT extends SQLServerTypeId.SelfIdentified("TEXT", 18)

  final case class TIME(precision: Int) extends SQLServerType(s"TIME($precision)")
  case object TIME extends SQLServerTypeId.HigherKinded(19) {
    val constructor = Constructor.Unary(PrecisionDateTimeParam(7), TIME(_))
  }

  case object TINYINT extends SQLServerTypeId.SelfIdentified("TINYINT", 20)

  case object UNIQUEIDENTIFIER extends SQLServerTypeId.SelfIdentified("UNIQUEIDENTIFIER", 21)

  final case class VARCHAR(length: Int) extends SQLServerType(s"VARCHAR($length)")
  case object VARCHAR extends SQLServerTypeId.HigherKinded(22) {
    val constructor = Constructor.Unary(LengthCharsParam(8000), VARCHAR(_))
  }

  ////

  private def LengthCharsParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Length (characters)", Formal.integer(Some(Ior.both(1, max)), None, None))

  private def PrecisionDateTimeParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Precision (decimal places)", Formal.integer(Some(Ior.both(0, max)), None, None))

  private def PrecisionFloatParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Precision (bits)", Formal.integer(Some(Ior.both(1, max)), None, None))
}

sealed trait SQLServerTypeId extends Product with Serializable {
  def ordinal: Int
}

object SQLServerTypeId {
  import SQLServerType._

  sealed abstract class SelfIdentified(spec: String, val ordinal: Int)
      extends SQLServerType(spec) with SQLServerTypeId

  sealed abstract class HigherKinded(val ordinal: Int) extends SQLServerTypeId {
    def constructor: Constructor[SQLServerType]
  }

  val allIds: Set[SQLServerTypeId] =
    Set(
      BIGINT,
      BINARY,
      CHAR,
      DATE,
      DATETIME,
      DATETIME2,
      DATETIMEOFFSET,
      DECIMAL,
      FLOAT,
      INT,
      MONEY,
      NCHAR,
      NUMERIC,
      NVARCHAR,
      REAL,
      SMALLDATETIME,
      SMALLINT,
      SMALLMONEY,
      TEXT,
      TIME,
      TINYINT,
      UNIQUEIDENTIFIER,
      VARCHAR)
}
