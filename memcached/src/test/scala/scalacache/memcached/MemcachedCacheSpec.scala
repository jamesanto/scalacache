package scalacache.memcached

import common.LegacyCodecCheckSupport
import org.scalatest.{ BeforeAndAfter, Matchers, FlatSpec }
import net.spy.memcached._

import scala.concurrent.duration._
import org.scalatest.concurrent.{ ScalaFutures, Eventually, IntegrationPatience }
import org.scalatest.time.{ Span, Seconds }

import java.nio.charset.Charset
import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scalacache.serialization.Codec

class MemcachedCacheSpec
    extends FlatSpec
    with Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with IntegrationPatience
    with LegacyCodecCheckSupport {

  val client = new MemcachedClient(AddrUtil.getAddresses("localhost:11211"))
  implicit val exe = ExecutionContext.Implicits.global

  def memcachedIsRunning = {
    try {
      client.get("foo")
      true
    } catch { case _: Exception => false }
  }

  def serialise[A](v: A)(implicit codec: Codec[A, Array[Byte]]): Array[Byte] = codec.serialize(v)

  if (!memcachedIsRunning) {
    alert("Skipping tests because Memcached does not appear to be running on localhost.")
  } else {

    before {
      client.flush()
    }

    behavior of "get"

    it should "return the value stored in Memcached" in {
      client.set("key1", 0, serialise(123))
      whenReady(MemcachedCache(client).get[Int]("key1")) { _ should be(Some(123)) }
    }

    it should "return None if the given key does not exist in the underlying cache" in {
      whenReady(MemcachedCache(client).get[Int]("non-existent-key")) { _ should be(None) }
    }

    it should "not block on deserialization failure for get" in {
      val codec = new Codec[String, Array[Byte]] {
        override def serialize(value: String): Array[Byte] = value.getBytes(Charset.forName("UTF-8"))
        override def deserialize(data: Array[Byte]): String = throw new RuntimeException("I should be easy to handle")
      }
      val memcachedClient = MemcachedCache(client)
      val key = "some-key"
      whenReady(
        memcachedClient.put[String](key, "some value", Some(3 seconds))(codec)
          .flatMap(_ => {
            memcachedClient.get[String](key)(codec)
              .recover {
                case _ =>
                  Some("another value")
              }
          })
      ) {
        _ should be(Some("another value"))
      }
    }

    behavior of "put"

    it should "store the given key-value pair in the underlying cache" in {

      whenReady(MemcachedCache(client).put("key2", 123, None)) { _ =>
        client.get("key2") should be(serialise(123))
      }
    }

    behavior of "put with TTL"

    it should "store the given key-value pair in the underlying cache" in {
      whenReady(MemcachedCache(client).put("key3", 123, Some(3 seconds))) { _ =>
        client.get("key3") should be(serialise(123))

        // Should expire after 3 seconds
        eventually(timeout(Span(4, Seconds))) {
          client.get("key3") should be(null)
        }
      }
    }

    it should "not block on deserialization failure for put" in {
      val codec = new Codec[String, Array[Byte]] {
        override def serialize(value: String): Array[Byte] = throw new RuntimeException("I should be easy to handle")
        override def deserialize(data: Array[Byte]): String = ???
      }
      val memcachedClient = MemcachedCache(client)
      whenReady(
        memcachedClient.put[String]("some-put-key", "some put value", Some(3 seconds))(codec)
          .recover {
            case _ => ()
          }
      ) {
        _ should be(())
      }
    }

    behavior of "remove"

    it should "delete the given key and its value from the underlying cache" in {
      client.set("key1", 0, 123)
      client.get("key1") should be(123)

      whenReady(MemcachedCache(client).remove("key1")) { _ =>
        client.get("key1") should be(null)
      }
    }

    legacySupportCheck { legacySerialization =>
      new MemcachedCache(client = client, useLegacySerialization = legacySerialization)
    }
  }

}

