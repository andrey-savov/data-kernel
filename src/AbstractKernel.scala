import java.io.{InputStream, OutputStream}
import java.sql.Timestamp

abstract class AbstractKernel[InputEvent, OutputEvent](val name: String)
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
  override def activate(sink: OutputSink): Unit = this.sink = sink

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

  /**
    * Serialize the state into an output stream in parallel, sharded by key.
    * Do not compress.
    * May be entered in parallel by multiple threads, each thread affinitized to a particular key.
    *
    * @param key     current shard key to serialize
    * @param numKeys total number shards that will be serialized
    * @param out     output stream specific to the shard
    * @throws NotImplementedError in case this is not implemented/supported
    */
  override def serializeParallel(key: Int, numKeys: Int, out: OutputStream): Unit =
    throw new NotImplementedError()

  /**
    * Deserialize the state from a byte stream in parallel, sharded by a key.
    * Must be called before activation.
    * Must be called after initialization.
    *
    * @param key     current shard key to deserialize
    * @param numKeys total number shards that will be deserialized
    * @param in      input stream specific to the shard
    * @throws NotImplementedError in case this is not implemented/supported
    */
  override def deserializeParallel(key: Int, numKeys: Int, in: InputStream): Unit =
    throw new NotImplementedError()
}
