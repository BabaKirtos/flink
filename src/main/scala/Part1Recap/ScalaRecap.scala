package Part1Recap

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object ScalaRecap extends App {

  println("Hello World!")

  // expressions are evaluated, instructions are executed
  // almost everything is an expression in Scala
  // whatever returns a Unit or () is an instruction
  val aBoolean: Boolean = 2 > 3
  val anExpression: String = if (aBoolean) "it was true" else "it was false"
  println(anExpression)

  // OOP
  // extending provides inheritance
  // we can extend at most 1 class
  // to avoid diamond problem, scala only supports single inheritance
  // ,but we can inherit from 0 or more traits (mixin, with right to left resolution)
  // this is done using `with` and trait
  // Note: if the inherited trait has a method with the same name and type
  // then it must extend the class and override that method as shown below
  class Animal {
    def whatType: String = "An Animal"
  }

  trait AdvancedAnimal extends Animal {
    override def whatType: String = "An advanced Animal"
  }

  class Cat(val name: String) extends Animal with AdvancedAnimal

  object Cat // companion object, equivalent to static in Java

  val newCat = new Cat("Meow")
  println(newCat.whatType)

  object Singleton { // a stand-alone singleton instance of the object
    val aValue = 2
  }

  println("The Singleton value: " + Singleton.aValue)

  // FP
  val incrementer: Int => Int = _ + 1
  println(incrementer(4))

  // HOFs
  val aNumList = List(1, 2, 3, 4, 5)
  val mapList = aNumList.map(x => List(x, x * 3)) // List[List[Int]]
  val aFlatMapList = aNumList.flatMap(x => List(x, x * 3)) // List[Int], ie flattened
  println(mapList)
  println(aFlatMapList)

  val aCharList = List('a', 'b', 'c')
  val comboList = aNumList.flatMap(num => aCharList.map(c => (num, c)))
  val comboFor = for {
    num <- aNumList
    ch <- aCharList
  } yield (ch, num)
  println(comboList)
  println(comboFor)

  // Futures
  val executorService = Executors.newFixedThreadPool(4)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  val aFuture: Future[Int] = Future {
    // code to be evaluated on a different thread
    ("123456789" * 10).map(_.asDigit).sum
  }

  // register a callback when it finishes
  def aPartialFunction(in: String): PartialFunction[Try[Int], Unit] = {
    case Success(value) => println(in + value)
    case Failure(exception) =>
      println(s"Got the following exception: ${exception.getMessage}")
  }

  aFuture.onComplete(aPartialFunction("Partial: "))

  val futureResult: Future[Int] = aFuture.map(_ * 2)
  futureResult.onComplete(aPartialFunction("Map: "))

  // implicits
  implicit val timeout = 500

  def setTimeout(f: () => Unit)(implicit tout: Int) = {
    Thread.sleep(tout)
    f()
  }

  setTimeout(() => println("timeout")) // timeout is automatically injected

  executorService.shutdown()

  // extension methods
  implicit class MyRichInt(num: Int) {
    def isEven: Boolean = num % 2 == 0
  }

  println(12.isEven)

  // conversions
  implicit def string2Animal(name: String): Cat = new Cat(name)

  // compiler instantiated a new Cat instance on this string
  // string2Animal("ImplicitMeow")
  val aString = "ImplicitMeow"

  println(aString.name)
}
