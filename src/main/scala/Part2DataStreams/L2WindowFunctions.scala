package Part2DataStreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.{Instant, ZoneId}
import scala.concurrent.duration._

object L2WindowFunctions {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant =
    Instant.parse("2024-08-28T00:00:00.000Z")

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player registers 2s after the server starts
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  // How many players were registered every 3 seconds
  // [0s - 3s), [3s - 6s), [6s - 9s)
  // this is done using count by windowAll

  val eventStream: DataStream[ServerEvent] = {
    env
      .fromCollection(events)
      .assignTimestampsAndWatermarks(
        // extract event timestamps and watermarks
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          // It means once we reject events before (watermark - 500 millis)
          .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
            override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long = {
              element.eventTime.toEpochMilli
            }
          })
      )
  }

  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  def getSecondsInt(millis: Long): Int =
    ((millis / 1000) % 100).toInt

  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    //                                             ^ input      ^ output  ^ window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      // input is already split based on our window
      // out of type Collector will collect result for each window computation
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(
        s"Window: [${getSecondsInt(window.getStart)} - ${getSecondsInt(window.getEnd)}) - $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  // Even more powerful window is ProcessWindowFunction
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(
        s"Window: [${getSecondsInt(window.getStart)} - ${getSecondsInt(window.getEnd)}) - $registrationEventCount")
    }
  }

  def demoCountByWindowV2(): Unit = {
    val registrationPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  // another alternative - aggregate function
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                            ^ input      ^ acc ^ output
    // initial acc value
    override def createAccumulator(): Long = 0L

    // how will the acc increase
    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push the final result from the acc type which can be richer
    override def getResult(accumulator: Long): Long = accumulator

    // takes 2 acc and returns the bigger acc
    override def merge(a: Long, b: Long): Long = a + b

    // note that there is no access to window or context like in other!!
  }

  def demoCountByWindowV3(): Unit = {
    val registrationPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  /*
  Keyed streams and window functions
   */
  // Each element will be assigned to a "mini-stream" based on its own key
  // They grouped by their key and same key streams runs on same machine
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)

  // For every key we will have a separate window allocation
  // Refer to the video for ASCII diagrams
  val threeSecondsTumblingWindows = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountInWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(
        s"$key: " +
          s"start - ${getSecondsInt(window.getStart)}, " +
          s"end - ${getSecondsInt(window.getEnd)}, " +
          s"count - ${input.size}")
    }
  }

  // Alternative - process functions for windows
  class CountInWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(
        s"$key: " +
          s"start = ${getSecondsInt(context.window.getStart)}, " +
          s"end = ${getSecondsInt(context.window.getEnd)}, " +
          s"count = ${elements.size}")
    }
  }

  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindows.apply(new CountInWindow)
    finalStream.print()
    env.execute()
  }

  def demoCountByTypeByWindowV2(): Unit = {
    val finalStream = threeSecondsTumblingWindows.process(new CountInWindowV2)
    finalStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    // demoCountByWindow()
    // demoCountByWindowV2()
    // demoCountByWindowV3()
    // demoCountByTypeByWindow()
    demoCountByTypeByWindowV2()
  }
}
