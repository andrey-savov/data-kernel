import java.sql.Timestamp

import scala.reflect.runtime.universe._

/**
  * Word count topology.
  */
class WordCount extends AbstractTopology[String, (String, Int)] {

  val countWords = new CountWords
  val splitWords = new SplitWords

  override def init(): Unit = {
    super.init()
    countWords.init(); splitWords.init()
  }

  override def activate(sink: ((String, Int), Timestamp) => Unit): Unit = {
    super.activate(sink)

    // Link & activate
    countWords.activate(sink)
    splitWords.activate(countWords.process)
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

  def topologyOutput(ev: (String, Int), ts: Timestamp): Unit = ev match {
    case (w, c) => println (s"$ts: $w -> $c")
  }

  // Initialize and activate the topology
  val topology = new WordCount
  topology.init()
  topology.activate(topologyOutput)

  // Feed lines from stdin
  var line: String = _
  while ({ line = StdIn.readLine(); line} != null) {
    topology.process(line, new Timestamp(System.currentTimeMillis()))
  }

  // Cleanup & exit
  topology.cleanup()
}
