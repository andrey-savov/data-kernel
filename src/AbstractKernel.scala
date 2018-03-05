import java.io.{InputStream, OutputStream}
import java.sql.Timestamp

abstract class AbstractKernel[InputEvent, OutputEvent]
  extends KernelTrait[InputEvent, OutputEvent] {

  protected var sink: (OutputEvent, Timestamp) => Unit = _

  /**
    * Initialize the kernel to initial state.
    * Intentionally paremeterless: global context is a concern of implementations.
    * This method will simply initialize the state to initial.
    */
  override def init(): Unit = {}

  /**
    * Activate the kernel with an output sink.
    *
    * @param sink callback to use to output
    */
  override def activate(sink: (OutputEvent, Timestamp) => Unit): Unit = this.sink = sink

  /**
    * Close a processing window.
    * Windows are defined through annotations.
    */
  override def closeWindow(ts: Timestamp): Unit = {}

  /**
    * Serialize the state into an output stream.
    * Do not compress.
    *
    * @param out output stream
    */
  override def serialize(out: OutputStream): Unit = {}

  /**
    * Deserialize the state from a byte stream.
    * Must be called before activation.
    * Must be called after initialization.
    *
    * @param in input stream
    */
  override def deserialize(in: InputStream): Unit = {}

  /**
    * Stop processing events and release any resources associated with the output sink.
    * Will emit any pending aggregate events into the output prior to deactivation.
    */
  override def deactivate(): Unit = {
    if (this.sink != null) {
      this.closeWindow(new Timestamp(System.currentTimeMillis()))
      this.sink = null
    }
  }

  /**
    * Cleanup any remaining state.
    * This will deactivate active kernels.
    */
  override def cleanup(): Unit = {
    this.deactivate()
  }
}
