package Part1Recap

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
  // but we can inherit from 0 or more traits (mixin, with right to left resolution)
  // this is done using `with` and trait
  // Note: if the inherited trait has a method with the same name and type
  // then it must extend the class and override that method as shown below
  class Animal {
    def whatType: String = "An Animal"
  }

  trait AdvancedAnimal extends Animal {
    override def whatType: String = "An advanced Animal"
  }

  class Cat extends Animal with AdvancedAnimal

  object Cat // companion object, equivalent to static in Java

  val newCat = new Cat
  println(newCat.whatType)

  object Singleton { // a stand alone singleton instance of the object
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

}
