package Part2DataStreams

// import implicit TypeInformation for the data of our DataStream

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object L1EssentialStreams {

  // Flink application template
  def applicationTemplate(): Unit = {

    // 1. execution env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. In between, add any type of computations
    // input
    val simpleNumberStream: DataStream[Int] = env.fromSequence(0, 999).map(_.toInt)
    // actions
    simpleNumberStream.print()

    // 3. execute using the env
    env.execute() // triggers all the computations in the graph
  }

  // transformation
  def demoTranformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val nums: DataStream[Int] = env.fromElements(((0 to 9)): _*)

    // checking default parallelism
    println("Current parallelism: " + env.getParallelism)

    // set parallelism
    env.setParallelism(2)

    // checking new parallelism
    println("New parallelism: " + env.getParallelism)

    // map
    val doubledNums: DataStream[Int] = nums.map(_ * 2)

    // flatMap
    val expandedNums: DataStream[Int] = nums.flatMap(x => List(x, x * 3))

    // filter
    val evenNums: DataStream[Int] = nums
      .filter(_ % 2 == 0)
      // you can set parallelism here as well
      .setParallelism(4)

    val finalData = expandedNums.writeAsText("output/expandedStream.txt") // creates a directory

    // we can set parallelism at sink as well
    finalData.setParallelism(3)

    env.execute()
  }

  // Exercise: FizzBuzz on Flink
  // - take a stream of 100 natural numbers
  // - for every number return fizz or buzz
  // - if num % 3 == 0 "fizz"
  // - if num % 5 == 0 "buzz"
  // - if both then "fizzbuzz"
  // print numbers which have "fizzbuzz"
  case class FizzBuzzResult(n: Int, output: String)

  def solution(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val naturalNums: DataStream[Int] = env.fromElements((1 to 100): _*)

    val partialFunc: PartialFunction[Int, String] = {
      case x if x % 3 == 0 && x % 5 == 0 => "FizzBuzz"
      case x if x % 3 == 0 => "Fizz"
      case x if x % 5 == 0 => "Buzz"
      case _ => "Nothing"
    }

    val outputTuple: DataStream[FizzBuzzResult] = naturalNums.map(x => FizzBuzzResult(x, partialFunc(x)))

    val fizzbuzzSet = outputTuple.filter(_.output == "FizzBuzz")

    fizzbuzzSet.map(x => (x.n, x.output)).print()

    //    fizzbuzzSet
    //      .map(x => (x.n, x.output))
    //      .writeAsText("output/FizzBuzz.txt")
    //      .setParallelism(1)

    // alternative to the `writeAsText` method is to add a Sink
    fizzbuzzSet.addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("output/streamingFizzBuzz.txt"),
          new SimpleStringEncoder[FizzBuzzResult]("UTF-8")
        ).build()
    ).setParallelism(1)

    env.execute()
  }

  // explicit transformations
  def demoExplicitTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val nums: DataStream[Long] = env.fromSequence(1, 100)

    // map
    val doubledNums: DataStream[Long] = nums.map(_ * 2)

    // low level map, explicit version
    val doubleNums_v2 = nums.map(new MapFunction[Long, Long] {
      // we can declare fields, methods, more complex computations
      override def map(value: Long): Long = value * 2
    })

    val expandedNums = nums.flatMap(x => Range.Long(1, x, 1).toList)

    // explicit flatMap
    val expandedNums_v2 = nums.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(n: Long, out: Collector[Long]): Unit = {
        // we can declare fields, methods, more complex computations
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style - pushes each element downstream
        }
      }
    })

    // most powerful is the process method
    // it's the most general process function in Flink
    val expanedNums_v3 = nums.process(new ProcessFunction[Long, Long] {
      override def processElement(n: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style - pushes each element downstream
        }
      }
    })

    // reduce
    // they happen on keyed streams
    val keyedNums: KeyedStream[Long, Boolean] = nums.keyBy(n => n % 2 == 0)
    val sumByKey = keyedNums.reduce(_ + _) // sum all element by key

    // explicit
    val sumByKey_v2 = keyedNums.reduce(new ReduceFunction[Long] {
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey_v2.print()

    env.execute()
  }


  def main(args: Array[String]): Unit = {
//    applicationTemplate()
//    demoTranformation()
//    solution()
    demoExplicitTransformation()
  }
}
