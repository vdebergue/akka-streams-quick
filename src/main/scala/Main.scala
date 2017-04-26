import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

import akka.stream._
import akka.stream.scaladsl._
import akka.actor.ActorSystem

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world")

    implicit val system = ActorSystem("quick-streams")
    implicit val materializer = ActorMaterializer()

    // Main code
    import ExampleStreams._

    // example of operations: filter, windowing, mapConcat ...
    // val letterCount = fastProducer
    //   .mapConcat[Char](word => word.toList)
    //   .filter(char => char != ' ')
    //   .groupBy(26, identity _)
    //   .scan(0L) { (count, letter) =>
    //     val newCount = count + 1
    //     println(s"Got $newCount for letter $letter")
    //     newCount
    //   }
    // letterCount.to(Sink.ignore).run

    /** Basics */
    val source = Source(List(1,2,3,4))
    val sink = Sink.foreach(println)
    val runnable = source.to(sink)
    runnable.run()

    // val flow = Flow[Int].map(x => x + 10).filter(x => x % 2 == 0)
    // source.via(flow).runWith(sink)

    // val foldingSink = Sink.fold[Int, Int](0)((x, acc) => x + acc)
    // val foldResult: Future[Int] = source.runWith(foldingSink)
    // val result = Await.result(foldResult, 15.seconds)
    // println(s"Sum is $result")

    /*
     * Simple slow/fast consumer associations
     * Question: How is the back pressure handled ?
     */

    // val graph = fastProducer.to(slowConsumer)
    // val graph = slowProducer.to(slowConsumer)
    // val graph = slowProducer.to(fastConsumer)
    // val graph = fastProducer.to(fastConsumer)


    /*
     * Push vs Pull Producers
     * Question: What happends when the consumer is not fast enough ?
     */

    // val graph = fastPushProducer.to(slowConsumer)
    // val graph = fastPushProducer.to(fastConsumer)

    /*
     * multiple consumers and producers associated
     */

     // val graph = (slowProducer merge fastProducer).to(slowConsumer)
     val graph = (slowProducer merge fastProducer).to(fastConsumer)

    /*
     * Async parallelism
     */

    // val graph = fastProducer
    //   .mapAsync(4) { x =>
    //     log("mapAsync")(x)
    //     Future {
    //       Thread.sleep(1000)
    //       x
    //     }(system.dispatcher)
    //   }
    //   .to(fastConsumer)


     /*
     * Error handling
     * Try the graph with and without the attributes
     */
    // val decider: Supervision.Decider = {
    //   case _: ArithmeticException => Supervision.Resume
    //   case _                      => Supervision.Stop
    // }
    // val graph = Source(0 to 10)
    //   .map(x => 100 / x)
    //   .map(_.toString)
    //   // .withAttributes(ActorAttributes.supervisionStrategy(decider))
    //   .to(fastConsumer)

    // run the graph
    graph.run()


    // Shutdown properly
    println("done ? Press [Enter]")
    readLine()
    materializer.shutdown()
    system.terminate().map { _ =>
      System.exit(0)
    }(scala.concurrent.ExecutionContext.global)
  }
}

object ExampleStreams {

  // Simple Source/Sink
  val slowProducer: Source[String, _] =
    Source
      .tick(0.second, 1.seconds, "slow")
      .map(log("slowPullProducer") _)

  val fastProducer: Source[String, _] =
    Source
      .tick(0.second, 100.millis, "fast")
      .map(log("fastPushProducer") _)

  val slowConsumer: Sink[String, _] =
    Sink.foreach { t =>
      log("slowConsumer")(t)
      Thread.sleep(1000)
    }

  val fastConsumer: Sink[String, _] =
    Sink.foreach { t =>
      log("fastConsumer")(t)
    }


  // Push Sources
  val fastPushProducer: Source[String, _] =
    Source.queue[String](bufferSize = 10, overflowStrategy = OverflowStrategy.dropTail)
      .mapMaterializedValue { sourceQueue =>
        val thread = new Thread() {
          override def run() {
            while(true) {
              sourceQueue.offer(log("fastPushProducer")("fastPush"))
              Thread.sleep(100)
            }
          }
        }
        thread.setDaemon(true)
        thread.start()
        sourceQueue
      }

  def log[T](name: String)(t: T): T = {
    val date = java.time.LocalTime.now()
    println(s"$date - $name [${Thread.currentThread().getName()}]: $t")
    t
  }
}
