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
    import Streams._

    /*
     * 1) Simple slow/fast consumer associations
     * Question: How is the back pressure handled ?
     */

    val graph = fastProducer.to(slowConsumer)
    // val graph = slowProducer.to(slowConsumer)
    // val graph = slowProducer.to(fastConsumer)
    // val graph = fastProducer.to(fastConsumer)

    /*
     * 2) Push vs Pull Producers
     * Question: What happends when the consumer is not fast enough ?
     */

    // val graph = fastPushProducer.to(slowConsumer)
    // val graph = fastPushProducer.to(fastConsumer)

    /*
     * 3) multiple consumers and producers associated
     * Question: What happends when the consumer is not fast enough ?
     */

    /*
     * 4) Async parallelism
     * Question: What happends when the consumer is not fast enough ?
     */



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

object Streams {

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
