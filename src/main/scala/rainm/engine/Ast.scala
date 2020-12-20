package rainm.engine

import rainm.engine.Value.Value

object Ast {

  trait Node

  trait SqlExpr extends Node

  trait SqlProjection extends SqlExpr

  trait BinaryOp extends SqlExpr {
    val left: SqlExpr
    val right: SqlExpr
  }

  trait Comparing extends BinaryOp

  case class Eq(left: SqlExpr, right: SqlExpr) extends Comparing

  case class Neq(left: SqlExpr, right: SqlExpr) extends Comparing

  case class Less(left: SqlExpr, right: SqlExpr) extends Comparing

  case class Greater(left: SqlExpr, right: SqlExpr) extends Comparing

  case class LessOrEqual(left: SqlExpr, right: SqlExpr) extends Comparing

  case class GreaterOrEqual(left: SqlExpr, right: SqlExpr) extends Comparing

  case class Or(left: SqlExpr, right: SqlExpr) extends SqlExpr

  case class And(left: SqlExpr, right: SqlExpr) extends SqlExpr

  case class Literal(value: Value[_]) extends SqlExpr with SqlProjection

  case class FieldIdent(qualify: Option[String], name: String) extends SqlExpr with SqlProjection {

    val key: String = qualify match {
      case None => name
      case Some(q) => q + "." + name
    }

    override def toString: String = key
  }

  case class StarProj() extends SqlProjection

  trait SqlAgg extends SqlExpr

  case class CountStar() extends SqlProjection with SqlAgg

  case class CountExpr(expr: SqlProjection, distinct: Boolean) extends SqlProjection with SqlAgg

  case class Sum(expr: SqlProjection, distinct: Boolean) extends SqlProjection with SqlAgg

  case class Avg(expr: SqlProjection, distinct: Boolean) extends SqlProjection with SqlAgg

  case class Min(expr: SqlProjection) extends SqlProjection with SqlAgg

  case class Max(expr: SqlProjection) extends SqlProjection with SqlAgg

  case class GroupBy(keys: Seq[SqlProjection]) extends Node

  case class OrderBy(keys: Seq[SqlProjection]) extends Node

  trait Relation extends Node

  case class TableRelation(name: String, alias: Option[String]) extends Relation

  case class TableWithJoin(table: TableRelation, joins: Seq[JoinRelation]) extends Relation

  case class JoinRelation(table: TableRelation, condition: Option[SqlExpr])

  case class Projection(sqlProj: SqlProjection, alias: Option[String]) extends Node

  case class SelectStmt(projections: Seq[Projection],
                        relations: Relation,
                        where: Option[SqlExpr],
                        groupBy: Option[GroupBy],
                        orderBy: Option[OrderBy],
                        limit: Option[(Option[Int], Int)]) extends Node

}
