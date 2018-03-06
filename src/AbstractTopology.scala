import java.sql.Timestamp
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Reusable aggregate of kernels, including queued event transfer between them.
  * Provides the ability to compose new data processing functionality from existing kernels
  * and/or topolgies.
  *
  * Provides the ability to employ threads in transferring events from kernel to kernel to compensate for
  * differences in event processing latencies.
  *
  * @tparam InputEvent input event type
  * @tparam OutputEvent output event type
  */
abstract class AbstractTopology[InputEvent, OutputEvent]
  extends AbstractKernel[InputEvent, OutputEvent] {

  //TODO: Construct topology via JSON descriptor

  /**
    * Abstraction of an <b>active<b> pipe: FIFO queue with a number of threads serving the queue.
    * @param length length/capacity of the pipe
    * @param num_pump_threads number of threads to serve the queue
    * @tparam Event type of event in the pipe
    */
  class Pipe[Event](val name: String, val length: Int, num_pump_threads: Int = 1) {

    //TODO: Instrumentation of the pipe: size, rates (i/o), first/last timestamp

    // Blocking queue
    private val queue = new ArrayBlockingQueue[(Event, Timestamp)](length)
    private var process_events = true

    /**
      * Input an element waiting if pipe is full.
      * @param e event to input
      */
    def input(e: Event, ts: Timestamp): Unit = {
      queue.put((e, ts))
    }

    /**
      * Activate the pipe with a sink factory.
      * @param output_sink_factory factory to generate output sinks for each thread
      */
    def activate(output_sink_factory: Int => (Event, Timestamp) => Unit): Unit = {
      // Start the dequeueing threads
      (0 until num_pump_threads).map(i => new Thread {
        override def run(): Unit = {
          val sink = output_sink_factory(i)
          while (process_events || !queue.isEmpty) {
            try {
              val (e, ts) = queue.take()
              sink(e, ts)
            } catch {
              case _: InterruptedException => {}
            }
          }
        }
      }).foreach(t => t.start())
    }

    def deactivate(): Unit = {
      process_events = false
    }
  }

}
