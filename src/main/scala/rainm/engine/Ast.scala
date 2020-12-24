package rainm.engine

import rainm.engine.Value.Value

object Ast {

  trait Node

  trait SqlExpr extends Node

  trait ProjectExpr extends SqlExpr

  trait BinaryOpExpr extends SqlExpr {
    val left: SqlExpr
    val right: SqlExpr
  }

  case class Eq(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class Neq(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class Less(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class Greater(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class LessOrEqual(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class GreaterOrEqual(left: SqlExpr, right: SqlExpr) extends BinaryOpExpr

  case class Or(left: SqlExpr, right: SqlExpr) extends SqlExpr

  case class And(left: SqlExpr, right: SqlExpr) extends SqlExpr

  case class Literal(value: Value[_]) extends SqlExpr with ProjectExpr

  case class Field(qualify: Option[String], name: String) extends SqlExpr with ProjectExpr {

    val key: String = qualify match {
      case None => name
      case Some(q) => q + "." + name
    }

    override def toString: String = key
  }

  case class StarProj() extends ProjectExpr

  trait SqlAgg extends SqlExpr

  case class CountStar() extends ProjectExpr with SqlAgg

  case class CountExpr(expr: ProjectExpr, distinct: Boolean) extends ProjectExpr with SqlAgg

  case class Sum(expr: ProjectExpr, distinct: Boolean) extends ProjectExpr with SqlAgg

  case class Avg(expr: ProjectExpr, distinct: Boolean) extends ProjectExpr with SqlAgg

  case class Min(expr: ProjectExpr) extends ProjectExpr with SqlAgg

  case class Max(expr: ProjectExpr) extends ProjectExpr with SqlAgg

  case class GroupBy(keys: Seq[ProjectExpr]) extends Node

  case class OrderBy(keys: Seq[ProjectExpr]) extends Node

  trait Relation extends Node

  case class TableRelation(name: String, alias: Option[String]) extends Relation

  case class TableWithJoin(tableRel: TableRelation, joins: Seq[JoinRelation]) extends Relation

  case class JoinRelation(tableRel: TableRelation, condition: Option[SqlExpr])

  case class Projection(expr: ProjectExpr, alias: Option[String]) extends Node

  case class SelectStmt(projections: Seq[Projection],
                        relations: Relation,
                        where: Option[SqlExpr],
                        groupBy: Option[GroupBy],
                        orderBy: Option[OrderBy],
                        limit: Option[(Option[Int], Int)]) extends Relation

}
