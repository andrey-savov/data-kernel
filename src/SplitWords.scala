import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

@EventCountWindow(60)
class SplitWords extends AbstractKernel[String, (String, Int)]("SplitWords") {

  // Local buffer that will hold the words and their local (line) counts
  private var buffer: ArrayBuffer[(String, Int)] = _

  /**
    * Initialize the kernel to initial state.
    */
  override def init(): Unit = {
    this.buffer = new ArrayBuffer[(String, Int)]()
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
    val local_count = ev
      .split("\\W+")
      .groupBy(s => s)
      .map { case (w, l) => (w, l.length) }
    local_count.foreach(buffer.append(_))
  }

  /**
    * Close a processing window.
    * Windows are defined through annotations.
    */
  override def closeWindow(ts: Timestamp): Unit = {
    super.closeWindow(ts)

    // Emit all buffered
    for (i <- this.buffer)
      this.sink(i, ts)

    // Clear
    this.buffer.clear()
  }

  /**
    * Serialize the state into an output stream.
    * Do not compress.
    *
    * @param out output stream
    */
  override def serialize(out: OutputStream): Unit = {
    val os = new ObjectOutputStream(out)
    os.writeObject(this.buffer)
  }

  /**
    * Deserialize the state from a byte stream.
    * Must be called before activation.
    * Must be called after initialization.
    *
    * @param in input stream
    */
  override def deserialize(in: InputStream): Unit = {
    val is = new ObjectInputStream(in)
    this.buffer = is.readObject().asInstanceOf[ArrayBuffer[(String, Int)]]
  }

  /**
    * Cleanup any remaining state.
    * This will deactivate active kernels.
    */
  override def cleanup(): Unit = {
    super.deactivate()
    this.buffer = null
  }
}

