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

    // 1) Simple slow/fast consumer associations
    // Question: How is the back pressure handled ?

    // val graph = fastProducer.to(slowConsumer)
    // val graph = slowProducer.to(slowConsumer)
    // val graph = slowProducer.to(fastConsumer)
    val graph = fastProducer.to(fastConsumer)


    // 2) Push vs Pull Producers


    // 3) multiple consumers and producers associated


    // 4) Async parallelism


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

  def log[T](name: String)(t: T): T = {
    val date = java.time.LocalTime.now()
    println(s"$date - $name [${Thread.currentThread().getName()}]: $t")
    t
  }
}
