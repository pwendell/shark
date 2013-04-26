package shark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import org.apache.hadoop.io.BytesWritable
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import shark.execution.ReduceKey
import shark.execution.serialization.ShuffleSerializer

class SerializationSuite extends FunSuite with ShouldMatchers {
  test("Serializing and deserializing from a stream") {
    val NUM_ITEMS = 5000
    val KEY_SIZE = 1000
    val VALUE_SIZE = 1000

    val initialItems =
      (1 to NUM_ITEMS).map {x =>
        val rkBytes = (1 to KEY_SIZE).map(_.toByte).toArray
        val valueBytes = (1 to VALUE_SIZE).map(_.toByte).toArray
        val rk = new ReduceKey(new BytesWritable(rkBytes))
        val value = new BytesWritable(valueBytes)
        (rk, value)
      }

    val bos = new ByteArrayOutputStream()
    val ser = new ShuffleSerializer()
    val serStream = ser.newInstance().serializeStream(bos)
    initialItems.map(serStream.writeObject(_))
    val bis = new ByteArrayInputStream(bos.toByteArray)
    val serInStream = ser.newInstance().deserializeStream(bis)

    initialItems.map{ x =>
      val output: (ReduceKey, BytesWritable) = serInStream.readObject()
      output should equal (x)
    }
  }

  test("Serializing and deserializing from a stream (with compression)") {
    val NUM_ITEMS = 1000
    val KEY_SIZE = 1000
    val VALUE_SIZE = 1000

    val initialItems =
      (1 to NUM_ITEMS).map {x =>
        val rkBytes = (1 to KEY_SIZE).map(_.toByte).toArray
        val valueBytes = (1 to VALUE_SIZE).map(_.toByte).toArray
        val rk = new ReduceKey(new BytesWritable(rkBytes))
        val value = new BytesWritable(valueBytes)
        (rk, value)
      }

    val bos = new ByteArrayOutputStream()
    val cBos = new LZFOutputStream(bos)
    val ser = new ShuffleSerializer()
    val serStream = ser.newInstance().serializeStream(cBos)
    initialItems.map(serStream.writeObject(_))
    serStream.close()
    val array = bos.toByteArray
    val bis = new ByteArrayInputStream(array)
    val cBis = new LZFInputStream(bis)
    val serInStream = ser.newInstance().deserializeStream(cBis)

    initialItems.map{ x =>
      val output: (ReduceKey, BytesWritable) = serInStream.readObject()
      output should equal (x)
    }
  }
}
