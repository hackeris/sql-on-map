package rainm.engine

import java.util.Date
import scala.annotation.tailrec

object Value {

  sealed trait Value[T] {
    def value: T

    override def toString: String = value.toString

    override def hashCode: Int = value.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case that: Value[T] => that.value.equals(value)
      case _ => false
    }
  }

  implicit class IntValue(val v: Int) extends Value[Int] {
    override def value: Int = v
  }

  implicit class DoubleValue(val v: Double) extends Value[Double] {
    override def value: Double = v
  }

  implicit class StringValue(val v: String) extends Value[String] {
    override def value: String = v
  }

  implicit class DateValue(val v: Date) extends Value[Date] {
    override def value: Date = v
  }

  implicit val valueSeqOrdering: Ordering[Seq[Value[_]]] = new Ordering[Seq[Value[_]]] {
    @tailrec
    override def compare(x: Seq[Value[_]], y: Seq[Value[_]]): Int = {

      def valueComparator(x: Value[_], y: Value[_]): Int = (x, y) match {
        case (x: IntValue, y: IntValue) => x.value.compareTo(y.value)
        case (x: DoubleValue, y: DoubleValue) => x.value.compareTo(y.value)
        case (x: StringValue, y: StringValue) => x.value.compareTo(y.value)
        case (x: DateValue, y: DateValue) => x.value.compareTo(y.value)
        case _ => throw new RuntimeException("comparing not supported")
      }

      (x, y) match {
        case (x1 :: Nil, y1 :: Nil) => valueComparator(x1, y1)
        case (x1 :: tailsX, y1 :: tailsY) =>
          val partial = valueComparator(x1, y1)
          if (partial != 0) {
            partial
          } else {
            compare(tailsX, tailsY)
          }
      }
    }
  }

  implicit val numericValue: Numeric[Value[_]] = new Numeric[Value[_]] {
    override def plus(x: Value[_], y: Value[_]): Value[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value + y.value
      case (x: DoubleValue, y: DoubleValue) => x.value + y.value
      case (x: IntValue, y: DoubleValue) => x.value + y.value
      case (x: DoubleValue, y: IntValue) => x.value + y.value
    }

    override def minus(x: Value[_], y: Value[_]): Value[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value - y.value
      case (x: DoubleValue, y: DoubleValue) => x.value - y.value
      case (x: IntValue, y: DoubleValue) => x.value - y.value
      case (x: DoubleValue, y: IntValue) => x.value - y.value
    }

    override def times(x: Value[_], y: Value[_]): Value[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value * y.value
      case (x: DoubleValue, y: DoubleValue) => x.value * y.value
      case (x: IntValue, y: DoubleValue) => x.value * y.value
      case (x: DoubleValue, y: IntValue) => x.value * y.value
    }

    override def negate(x: Value[_]): Value[_] = x match {
      case (x: IntValue) => -x.value
      case (x: DoubleValue) => -x.value
    }

    override def fromInt(x: Int): Value[_] = x

    override def toInt(x: Value[_]): Int = x match {
      case x: IntValue => x.value
      case x: DoubleValue => x.value.toInt
    }

    override def toLong(x: Value[_]): Long = x match {
      case x: IntValue => x.value
      case x: DoubleValue => x.value.toLong
    }

    override def toFloat(x: Value[_]): Float = x match {
      case x: IntValue => x.value
      case x: DoubleValue => x.value.toFloat
    }

    override def toDouble(x: Value[_]): Double = x match {
      case x: IntValue => x.value
      case x: DoubleValue => x.value
    }

    override def compare(x: Value[_], y: Value[_]): Int
    = valueSeqOrdering.compare(Seq(x), Seq(y))
  }

}
