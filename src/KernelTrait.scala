import java.io.{InputStream, OutputStream}
import java.sql.Timestamp

import scala.annotation.Annotation

/**
  * Annotation used to require time ordered input of events.
  * @param value Whether the kernel requires a timeo-ordered series of events
  */
class RequiresTimeOrderedInput(value: Boolean = false) extends Annotation

/**
  * Annotation used to indicate that processing happens on a window.
  * @param width_sec width of the window in seconds
  * @param slide_sec slide interval in seconds
  */
class SlidingTimeWindow(width_sec: Int, slide_sec: Int) extends Annotation

/**
  * Tumbling window annotation.
  * @param value width of the window in seconds
  */
class TumblingTimeWindow(value: Int) extends SlidingTimeWindow(value, value)

/**
  * Annotation used to indicate time windows periods (seconds) based on wall-clock.
  * @param value wall-clock time window in seconds
  */
class PeriodicWallClockWindow(value: Int) extends Annotation

/**
  * Annotation used to indicate processing windows based on count of events processed by the kernel.
  * @param value count of events that will close the window and start a new one
  */
class EventCountWindow(value: Int) extends Annotation

/**
  * Trait for kernels implementing stateful processing in a reactive way.
  * Each kernel processes events for a single key of the input and keys are not a concern of the kernel.
  *
  * @tparam InputEvent Type of the input events this kernel can process.
  * @tparam OutputEvent Type of the output events this kernel emits.
  */
trait KernelTrait[InputEvent, OutputEvent] {

  /**
    * Initialize the kernel to initial state.
    * Intentionally paremeterless: global context is a concern of implementations.
    * This method will simply initialize the state to initial.
    */
  def init(): Unit

  /**
    * Activate the kernel with an output sink.
    * @param sink callback to use to output
    */
  def activate(sink: (OutputEvent, Timestamp) => Unit): Unit

  /**
    * Process individual event.
    * This may or may not result in output event sent to the sink.
    * Only active kernels will process events.
    * @param ev event to process
    * @param ts timestamp of the event
    */
  def process(ev: InputEvent, ts: Timestamp): Unit

  /**
    * Close a processing window.
    * Windows are defined through annotations.
    * @param ts timestamp of the window closing.
    */
  def closeWindow(ts: Timestamp): Unit

  /**
    * Serialize the state into an output stream.
    * Do not compress.
    * @param out output stream
    */
  def serialize(out: OutputStream): Unit

  /**
    * Deserialize the state from a byte stream.
    * Must be called before activation.
    * Must be called after initialization.
    * @param in input stream
    */
  def deserialize(in: InputStream): Unit

  /**
    * Stop processing events and release any resources associated with the output sink.
    * Will emit any pending aggregate events into the output prior to deactivation.
    */
  def deactivate(): Unit

  /**
    * Cleanup any remaining state.
    * This will deactivate active kernels.
    */
  def cleanup(): Unit
}
