package rainm.engine

import rainm.engine.Ast._
import rainm.engine.Value._

object Engine {

  type Row = Map[String, Value[_]]
  type Table = Seq[Row]

  trait DataBase {
    def table(name: String): Table
  }

  implicit class MapAsDataBase(map: Map[String, Table]) extends DataBase {
    override def table(name: String): Table = map(name)
  }

  def query(database: DataBase, sql: String): Table = {
    val ast = Parser.parse(sql)
    ast match {
      case Some(r: SelectStmt) =>
        execute(database, r)
      case _ =>
        throw new RuntimeException("Illegal ast")
    }
  }

  def execute(database: DataBase, sql: SelectStmt): Table = {
    sql match {
      case SelectStmt(s, f, w, g, o, l) =>
        database.from(f)
          .where(w)
          .group(g)
          .agg(s)
          .select(s)
          .orderBy(o)
          .limit(l)
    }
  }

  implicit class DataBaseOp(database: DataBase) {
    def from(relation: Relation): Table = relation match {
      case rel: TableRelation =>
        selectAll(rel)
      case TableWithJoin(table, joins) =>
        joinWith(selectAll(table), joins)
      case sql: SelectStmt =>
        execute(database, sql)
    }

    def applyAlias(table: Table, alias: String): Table =
      table.map(row => applyAlias(row, alias))

    def applyAlias(row: Row, alias: String): Row = {
      row.collect({ case (_1, _2) => (alias + "." + _1, _2) }).toMap
    }

    def joinWith(table: Table, joins: Seq[JoinRelation]): Table = {
      joins match {
        case Nil => table
        case join :: tails =>
          joinWith(joinWithSingle(table, join), tails)
      }
    }

    def joinWithSingle(table: Table, join: JoinRelation): Table = {
      val right = selectAll(join.tableRel)
      val joined = table.flatMap(row => right.map(rightRow => row ++ rightRow))
      join.condition match {
        case Some(expr) =>
          joined.filter(row => evalWhereOnRow(row, expr))
        case None =>
          joined
      }
    }

    def selectAll(rel: TableRelation): Table = (rel.name, rel.alias) match {
      case (name, None) => database.table(name)
      case (name, Some(alias)) => applyAlias(database.table(name), alias)
    }
  }

  implicit class TableOp(table: Table) {

    def group(groupBy: Option[GroupBy]): Seq[Table] =
      groupBy match {
        case None => Seq(table)
        case Some(expr: GroupBy) => evalGroupBy(table, expr)
      }

    def where(where: Option[SqlExpr]): Table =
      where match {
        case None => table
        case Some(expr: SqlExpr) => table.filter(row => evalWhereOnRow(row, expr))
      }

    def orderBy(orderBy: Option[OrderBy]): Table =
      orderBy match {
        case None => table
        case Some(expr: OrderBy) => evalOrderBy(table, expr)
      }

    def limit(limit: Option[(Option[Int], Int)]): Table =
      limit match {
        case None => table
        case Some(x: (Option[Int], Int)) => x match {
          case (None, offset: Int) => table.take(offset)
          case (Some(offset: Int), count: Int) => table.slice(offset, offset + count)
        }
      }
  }

  implicit class GroupedTable(tables: Seq[Table]) {

    def isAgg(projection: Projection): Boolean = projection.expr match {
      case _: SqlAgg => true
      case _ => false
    }

    def select(projections: Seq[Projection]): Table =
      tables.map(evalProjections(_, projections))
        .reduce(_ ++ _)

    def agg(projections: Seq[Projection]): Seq[Table] =
      if (projections.exists(isAgg)) {
        tables.map(evalAggregateProjections(_, projections))
      } else {
        tables
      }

  }

  def evalAggregateProjections(input: Table, projections: Seq[Projection]): Table = {
    val result = projections.map({
      case Projection(e, alias) => e match {
        case e: SqlAgg => alias match {
          case None => evalAggFunction(input, e)
          case Some(alias: String) =>
            Map[String, Value[_]](alias -> evalAggFunction(input, e).values.head)
        }
        case e: Field =>
          input.head.collect({
            case (e.key, _2) => (e.key, _2)
          })
        case _: StarProj => input.head
      }
    })
    Seq(result.flatten.toMap)
  }

  def evalProjections(input: Table, expr: Seq[Projection]): Seq[Row] =
    input.map(evalProjectionsOnRow(_, expr))

  def evalProjectionsOnRow(row: Row, projections: Seq[Projection]): Row = {
    val pros = projections.map(proj => projector(proj)(row))
    pros.reduce(_ ++ _)
  }

  def projector(proj: Projection): Row => Row =
    (proj.expr, proj.alias) match {
      case (StarProj(), _) =>
        row => row
      case (f: Field, alias) => alias match {
        case None =>
          row => Map(f.key -> row(f.key))
        case Some(alias: String) =>
          row => Map(alias -> row(f.key))
      }
      case (l: Literal, alias) => alias match {
        case None =>
          _ => Map(l.value.toString -> l.value)
        case Some(alias: String) =>
          _ => Map(alias -> l.value)
      }
      case (_: SqlAgg, _) =>
        row => row
      case _ =>
        throw new RuntimeException("Unsupported projection")
    }

  def evalWhereOnRow(row: Row, expr: SqlExpr): Boolean = {
    def evalBinaryOp(r: Row, expr: BinaryOpExpr, f: (Value[_], Value[_]) => Boolean): Boolean = {
      (expr.left, expr.right) match {
        case (left: Literal, right: Literal) => f(left.value, right.value)
        case (left: Literal, right: Field) => f(left.value, right.of(r))
        case (left: Field, right: Literal) => f(left.of(r), right.value)
        case (left: Field, right: Field) => f(left.of(r), right.of(r))
      }
    }

    def eq(a: Value[_], b: Value[_]): Boolean = a == b

    def neq(a: Value[_], b: Value[_]): Boolean = a != b

    def lt(a: Value[_], b: Value[_])(implicit ord: Value[_] => Ordered[Value[_]]): Boolean = a < b

    def gt(a: Value[_], b: Value[_])(implicit ord: Value[_] => Ordered[Value[_]]): Boolean = a > b

    def lte(a: Value[_], b: Value[_])(implicit ord: Value[_] => Ordered[Value[_]]): Boolean = a <= b

    def gte(a: Value[_], b: Value[_])(implicit ord: Value[_] => Ordered[Value[_]]): Boolean = a >= b

    expr match {
      case expr: Eq => evalBinaryOp(row, expr, eq)
      case expr: Neq => evalBinaryOp(row, expr, neq)
      case expr: Less => evalBinaryOp(row, expr, lt)
      case expr: Greater => evalBinaryOp(row, expr, gt)
      case expr: LessOrEqual => evalBinaryOp(row, expr, lte)
      case expr: GreaterOrEqual => evalBinaryOp(row, expr, gte)
      case Or(left, right) => evalWhereOnRow(row, left) || evalWhereOnRow(row, right)
      case And(left, right) => evalWhereOnRow(row, left) && evalWhereOnRow(row, right)
    }
  }

  def evalAggFunction(input: Table, func: SqlAgg): Row = {
    func match {
      case CountStar() => Map("count(*)" -> input.size)
      case CountExpr(e: Field, distinct: Boolean) =>
        if (distinct) {
          val count = input.map(row => e.of(row)).distinct.size
          Map("count(distinct " + e + ")" -> count)
        } else {
          Map("count(" + e + ")" -> input.size)
        }
      case CountExpr(e: Literal, distinct: Boolean) =>
        if (distinct) Map("count(distinct " + e.value + ")" -> 1)
        else Map("count(" + e.value + ")" -> input.size)
      case Max(e: Field) =>
        Map("max(" + e + ")" -> input.maxBy(row => e.of(row)).head._2)
      case Min(e: Field) =>
        Map("min(" + e + ")" -> input.minBy(row => e.of(row)).head._2)
      case Max(e: Literal) => Map("max(" + e.value + ")" -> e.value)
      case Min(e: Literal) => Map("min(" + e.value + ")" -> e.value)
      case Sum(e: Field, distinct: Boolean) =>
        if (distinct) {
          val sum = input.map(row => e.of(row)).distinct.sum
          Map("sum(distinct " + e + ")" -> sum)
        } else {
          val sum = input.map(row => e.of(row)).sum
          Map("sum(distinct " + e + ")" -> sum)
        }
      case Sum(e: Literal, distinct: Boolean) =>
        if (distinct) {
          Map("sum(distinct " + e.value + ")" -> e.value)
        } else {
          e.value match {
            case e: IntValue => Map("sum(" + e.value + ")" -> e.value * input.size)
            case e: DoubleValue => Map("sum(" + e.value + ")" -> e.value * input.size)
            case _ => throw new RuntimeException("Cannot perform sum()")
          }
        }
      case Avg(e: Field, distinct: Boolean) =>
        val sum =
          if (distinct) input.map(row => e.of(row)).distinct.sum
          else input.map(row => e.of(row)).sum
        val n =
          if (distinct) input.map(row => e.of(row)).distinct.size
          else input.size
        val pref =
          if (distinct) "distinct " else ""
        sum.value match {
          case s: IntValue => Map("sum(" + pref + e + ")" -> s.value / n)
          case s: DoubleValue => Map("sum(" + pref + e + ")" -> s.value / n)
        }
      case Avg(e: Literal, distinct: Boolean) =>
        if (distinct) {
          Map("avg(distinct " + e.value + ")" -> e.value)
        } else {
          Map("avg(" + e.value + ")" -> e.value)
        }
    }
  }

  def evalGroupBy(table: Table, expr: GroupBy): Seq[Table] = {
    val keys = keysOf(Left(expr))
    table.groupBy(row => keys.map(k => row(k))).values.toSeq
  }

  def evalOrderBy(table: Table, expr: OrderBy): Table = {
    val keys = keysOf(Right(expr))
    table.sortBy(row => keys.map(k => row(k)))
  }

  def keysOf(expr: Either[GroupBy, OrderBy]): Seq[String] = {
    val keys = expr match {
      case Left(g) => g.keys
      case Right(o) => o.keys
    }
    keys.map({ case x: Field => x.key })
  }

  trait FieldGetter {
    def of(row: Row): Value[_]
  }

  implicit class FieldKeyGetter(key: String) extends FieldGetter {
    def of(row: Row): Value[_] = row(key)
  }

  implicit class FieldIdentGetter(ident: Field) extends FieldGetter {
    def of(row: Row): Value[_] =
      if (row.contains(ident.key)) row(ident.key)
      else row(ident.name)
  }

}
