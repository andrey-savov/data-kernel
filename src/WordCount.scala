import java.sql.Timestamp

import scala.reflect.runtime.universe._

import scala.io.StdIn
object WordCount extends App {
  val countWords = new CountWords
  val splitWords = new SplitWords

  // TODO: Read type annotations and spin windowing threads or counters.

  countWords.init(); splitWords.init()

  def topologyOutput(ev: Iterable[(String, Int)], ts: Timestamp): Unit =
    ev.foreach{ case (w, c) => println(s"$ts: $w -> $c")}

  // Link & activate
  countWords.activate(topologyOutput)
  splitWords.activate(countWords.process)

  def topologyInput(ev: String): Unit = splitWords.process(ev, new Timestamp(System.currentTimeMillis()))

  // Feed lines from stdin
  var line: String = _
  while ({ line = StdIn.readLine(); line} != null) {
    topologyInput(line)
  }

  splitWords.cleanup(); countWords.cleanup()
}
