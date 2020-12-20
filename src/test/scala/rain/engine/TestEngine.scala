package rain.engine

import org.junit.Test
import rainm.engine.Engine
import rainm.engine.Value.{DataBase, Row, Table}

class TestEngine {

  def table1(): Table = {
    val row1: Row = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val row2: Row = Map("a" -> 2, "b" -> 3, "c" -> 4)
    val row3: Row = Map("a" -> 3, "b" -> 4, "c" -> 5)
    val row4: Row = Map("a" -> 4, "b" -> 5, "c" -> 6)
    val row5: Row = Map("a" -> 5, "b" -> 6, "c" -> 7)
    Seq(row1, row2, row3, row4, row5)
  }

  def tableUser(): Table = {
    val row1: Row = Map("id" -> 1, "name" -> "Jack1", "c" -> 2)
    val row2: Row = Map("id" -> 2, "name" -> "Jack2", "c" -> 4)
    val row3: Row = Map("id" -> 3, "name" -> "Jack3", "c" -> 6)
    val row4: Row = Map("id" -> 4, "name" -> "Jack4", "c" -> 8)
    val row5: Row = Map("id" -> 5, "name" -> "Jack5", "c" -> 10)
    Seq(row1, row2, row3, row4, row5)
  }

  def tablePost(): Table = {
    val row1: Row = Map("id" -> 1, "user_id" -> 1, "title" -> "of 1")
    val row2: Row = Map("id" -> 2, "user_id" -> 2, "title" -> "of 2")
    val row3: Row = Map("id" -> 3, "user_id" -> 3, "title" -> "of 3")
    val row4: Row = Map("id" -> 4, "user_id" -> 4, "title" -> "of 4")
    val row5: Row = Map("id" -> 5, "user_id" -> 5, "title" -> "of 5")
    Seq(row1, row2, row3, row4, row5)
  }

  def tableComment(): Table = {
    val row1: Row = Map("id" -> 1, "post_id" -> 1, "content" -> "comment 1")
    val row2: Row = Map("id" -> 2, "post_id" -> 2, "content" -> "comment 2")
    val row3: Row = Map("id" -> 3, "post_id" -> 2, "content" -> "comment 3 2")
    Seq(row1, row2, row3)
  }

  @Test
  def singleTableTest(): Unit = {

    val table = table1()

    def query(table: Table, sql: String): Table = {
      val db: DataBase = Map("table" -> table)
      Engine.query(db, sql)
    }

    var result = query(table, "select b, a from table")
    print(result)

    result = query(table, "select table.b, a from table where c < 5")
    print(result)

    result = query(table, "select u.b, u.a from table as u where u.c < 5")
    print(result)

    result = query(table, "select max(b), min(a) from table")
    print(result)
  }

  @Test
  def joinTest(): Unit = {

    val users = tableUser()
    val posts = tablePost()
    val comments = tableComment()
    val db: DataBase = Map(
      "user" -> users,
      "post" -> posts,
      "comment" -> comments
    )

    var result = Engine.query(db, "select * from user")
    print(result)

    result = Engine.query(db, "select u.id, u.name, p.title " +
      "from user as u join post as p on u.id = p.user_id")
    print(result)

    result = Engine.query(db,
      "select u.id, u.name, p.title " +
        "from user as u join post as p on u.id = p.user_id " +
        "where u.id <= 3")
    print(result)

    result = Engine.query(db,
      "select u.id, p.id, c.id, u.name, p.title, c.content " +
        "from user as u " +
        "join post as p on u.id = p.user_id " +
        "join comment as c on c.post_id = p.id " +
        "where u.id <= 3")
    print(result)
  }

  def print(table: Table): Unit = {
    table.foreach(row => {
      println(row)
    })
    println("--------------------------")
  }

}
