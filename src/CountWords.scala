import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

@PeriodicWallClockWindow(10)
class CountWords extends AbstractKernel[(String, Int), (String, Int)] {

  private var map: mutable.OpenHashMap[String, AtomicInteger] = _

  /**
    * Initialize the map.
    */
  override def init(): Unit = {
    super.init()
    map = new mutable.OpenHashMap[String, AtomicInteger](10 * 1000)
  }

  /**
    * Process individual event.
    * This may or may not result in output event sent to the sink.
    * Only active kernels will process events.
    *
    * @param ev event to process
    * @param ts timestamp of the event
    */
  override def process(ev: (String, Int), ts: Timestamp): Unit = ev match {
    case (word, count) => this.map
      .getOrElseUpdate(word, new AtomicInteger(count - 1))
      .incrementAndGet()
  }

  /**
    * Serialize the state into an output stream.
    * Do not compress.
    *
    * @param out output stream
    */
  override def serialize(out: OutputStream): Unit = {
    super.serialize(out)
    val os = new ObjectOutputStream(out)
    os.writeObject(this.map)
  }

  /**
    * Deserialize the state from a byte stream.
    * Must be called before activation.
    * Must be called after initialization.
    *
    * @param in input stream
    */
  override def deserialize(in: InputStream): Unit = {
    super.deserialize(in)
    val is = new ObjectInputStream(in)
    this.map = is.readObject().asInstanceOf[mutable.OpenHashMap[String, AtomicInteger]]
  }

  /**
    * Close a processing window.
    * Windows are defined through annotations.
    */
  override def closeWindow(ts: Timestamp): Unit = {
    super.closeWindow(ts)
    this.map.map{ case (w, ac) => (w, ac.get())}.foreach(this.sink(_, ts))
    this.map.clear()
  }
}
