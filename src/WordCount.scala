import java.sql.Timestamp
import scala.compiletime.uninitialized

/**
  * Word count topology.
  */
class WordCount extends AbstractTopology[String, (String, Int)]("WordCount") {

  private val countWords = new CountWords
  private val pipe = new Pipe[(String, Int)](name = "Bridge", length = 1, num_pump_threads = 1)
  private val splitWords = new SplitWords

  override def init(): Unit = {
    super.init()
    countWords.init(); splitWords.init()
  }

  override def activate(sink: OutputSink): Unit = {
    super.activate(sink)

    def deadEnd(event: (String, Int), ts: Timestamp): Unit =
      println("Should not have been called")

    def sinkFactory(thread_index: Int): OutputSink = {
      if (thread_index == 0) countWords.process
      else deadEnd
    }

    // Link & activate
    countWords.activate(sink)
    pipe.activate(sinkFactory)
    splitWords.activate(pipe.input)
  }

  override def deactivate(): Unit = {
    super.deactivate()

    // Deactivate in reverse order
    splitWords.deactivate()
    pipe.deactivate()
    countWords.deactivate()
  }

  /**
    * Process individual event.
    * This may or may not result in output event sent to the sink.
    * Only active kernels will process events.
    *
    * @param ev event to process
    * @param ts timestamp of the event
    */
  override def process(ev: String, ts: Timestamp): Unit = {
    splitWords.process(ev, new Timestamp(System.currentTimeMillis()))
  }

  override def cleanup(): Unit = {
    super.cleanup()
    splitWords.cleanup(); countWords.cleanup()
  }
}

import scala.io.StdIn
object WordCount extends App {

  // TODO: Read type annotations and spin windowing threads or counters.

  private def topologyOutput(ev: (String, Int), ts: Timestamp): Unit = ev match {
    case (w, c) => println (f"$ts: $w -> $c")
  }

  // Initialize and activate the topology
  private val topology = new WordCount
  topology.init()
  topology.activate(topologyOutput)

  // Feed lines from stdin
  private var line: String = uninitialized
  while ({ line = StdIn.readLine(); line} != null) {
    topology.process(line, new Timestamp(System.currentTimeMillis()))
  }

  // Cleanup & exit
  topology.cleanup()
}
