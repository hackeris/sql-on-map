package rainm.engine

import rainm.engine.Ast._

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

object Parser extends StandardTokenParsers {

  class SqlLexical extends StdLexical {

    case class FloatLit(chars: String) extends Token {
      override def toString: String = chars
    }

    def identity: Parser[Token] =
      identChar ~ rep(identChar | digit) ^^ {
        case first ~ rest => processIdent(first :: rest mkString "")
      }

    def singleQuoteString: Parser[StringLit] =
      '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ {
        case '\'' ~ chars ~ '\'' => StringLit(chars mkString "")
      }

    def stringLiteral: Parser[StringLit] =
      singleQuoteString

    def numericLiteral: Parser[Token] =
      rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
        case i ~ None => NumericLit(i mkString "")
        case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
      }

    override def token: Parser[Token] = {
      identity | numericLiteral | stringLiteral | delim |
        EofCh ^^^ EOF |
        '\'' ~> failure("unclosed string literal") |
        '\"' ~> failure("unclosed string literal") |
        failure("illegal character")
    }

  }

  override val lexical = new SqlLexical()

  val functions = Seq("count", "sum", "avg", "min", "max", "substring", "extract")
  lexical.reserved += (
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "join", "asc", "desc", "from", "on", "not", "having", "distinct",
    "case", "when", "then", "else", "end", "for", "from", "exists", "between", "like", "in",
    "year", "month", "day", "null", "is", "date", "interval", "group", "order",
    "date", "left", "right", "outer", "inner"
  )
  lexical.reserved ++= functions
  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";"
  )

  def floatLit: Parser[String] =
    elem("decimal", x => x.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  def literal: Parser[SqlProjection] = {
    numericLit ^^ (i => Literal(i.toInt)) |
      stringLit ^^ (s => Literal(s)) |
      floatLit ^^ (f => Literal(f.toDouble)) |
      "null" ^^ (_ => Literal(null))
  }

  def fieldIdentity: Parser[FieldIdent] =
    ident ~ opt("." ~> ident) ^^ {
      case table ~ Some(field) =>
        FieldIdent(Option(table), field)
      case column ~ None =>
        FieldIdent(None, column)
    }

  def binaryOp: Parser[String] = "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="

  def primaryWhereExpr: Parser[SqlExpr] =
    (literal | fieldIdentity) ~ binaryOp ~ (literal | fieldIdentity) ^^ {
      case left ~ "=" ~ right => Eq(left, right)
      case left ~ "<>" ~ right => Neq(left, right)
      case left ~ "!=" ~ right => Neq(left, right)
      case left ~ "<" ~ right => Less(left, right)
      case left ~ ">" ~ right => Greater(left, right)
      case left ~ "<=" ~ right => LessOrEqual(left, right)
      case left ~ ">=" ~ right => GreaterOrEqual(left, right)
    } | "(" ~> expr <~ ")"

  def andExpr: Parser[SqlExpr] =
    primaryWhereExpr * ("and" ^^^ { (a: SqlExpr, b: SqlExpr) => And(a, b) })

  def orExpr: Parser[SqlExpr] =
    andExpr * ("or" ^^^ { (a: SqlExpr, b: SqlExpr) => Or(a, b) })

  def expr: Parser[SqlExpr] = orExpr

  def where: Parser[SqlExpr] = "where" ~> expr

  def projectionStatements: Parser[Seq[Projection]] = repsep(projection, ",")

  def projection: Parser[Projection] =
    "*" ^^ (_ => Projection(StarProj(), None)) |
      primarySelectExpr ~ opt("as" ~> ident) ^^ {
        case expr ~ alias => Projection(expr, alias)
      }

  def selectLiteral: Parser[SqlProjection] = literal

  def selectIdent: Parser[SqlProjection] =
    ident ~ opt("." ~> ident) ^^ {
      case table ~ Some(b: String) =>
        FieldIdent(Option(table), b)
      case column ~ None =>
        FieldIdent(None, column)
    }

  def singleSelectExpr: Parser[SqlProjection] =
    selectLiteral | selectIdent

  def primarySelectExpr: Parser[SqlProjection] =
    singleSelectExpr | knowFunction

  def knowFunction: Parser[SqlProjection] = {
    "count" ~> "(" ~> projInCount <~ ")" |
      "min" ~> "(" ~> singleSelectExpr <~ ")" ^^ (p => Min(p)) |
      "max" ~> "(" ~> singleSelectExpr <~ ")" ^^ (p => Max(p)) |
      "sum" ~> "(" ~> (opt("distinct") ~ singleSelectExpr) <~ ")" ^^ { case d ~ e => Sum(e, d.isDefined) } |
      "avg" ~> "(" ~> (opt("distinct") ~ singleSelectExpr) <~ ")" ^^ { case d ~ e => Avg(e, d.isDefined) }
  }

  def projInCount: Parser[SqlProjection] =
    "*" ^^ (_ => CountStar()) |
      opt("distinct") ~ singleSelectExpr ^^ {
        case d ~ e => CountExpr(e, d.isDefined)
      }

  def fromStatements: Parser[Relation] =
    "from" ~ relation ~ rep(joinRelation) ^^ {
      case _ ~ relation ~ Nil => relation
      case _ ~ relation ~ joins => TableWithJoin(relation, joins)
    }

  def relation: Parser[TableRelation] =
    ident ~ opt("as") ~ opt(ident) ^^ {
      case ident ~ _ ~ alias => TableRelation(ident, alias)
    }

  def joinRelation: Parser[JoinRelation] =
    "join" ~ ident ~ opt("as") ~ opt(ident) ~ opt("on") ~ opt(expr) ^^ {
      case _ ~ ident ~ _ ~ alias ~ _ ~ expr => JoinRelation(TableRelation(ident, alias), expr)
    }

  def groupStatements: Parser[GroupBy] =
    "group" ~> "by" ~> rep1sep(selectIdent, ",") ^^ (keys => GroupBy(keys))

  def orderByExpr: Parser[OrderBy] =
    "order" ~> "by" ~> rep1sep(selectIdent, ",") ^^ (keys => OrderBy(keys))

  def limit: Parser[(Option[Int], Int)] =
    "limit" ~> opt(numericLit <~ ",") ~ numericLit ^^ {
      case None ~ size => (None, size.toInt)
      case Some(b: String) ~ size => (Some(b.toInt), size.toInt)
    }

  def select: Parser[SelectStmt] =
    "select" ~>
      projectionStatements ~
      fromStatements ~
      opt(where) ~
      opt(groupStatements) ~
      opt(orderByExpr) ~
      opt(limit) ~
      opt(";") ^^ {
      case p ~ f ~ w ~ g ~ o ~ l ~ _ => SelectStmt(p, f, w, g, o, l)
    }

  def parse(sql: String): Option[SelectStmt] =
    phrase(select)(new lexical.Scanner(sql)) match {
      case Success(r, q) => Option(r)
      case x => throw new IllegalArgumentException(x.toString)
    }
}
