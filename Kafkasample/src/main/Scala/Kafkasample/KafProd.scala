package Kafkasample

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties

import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException

object KafProd {

  private val logger = LogManager.getLogger()

  def main(args: Array[String]): Unit = {


    val topicName = "topicName"
    val numb1=10
    logger.info("Starting HelloProducer...")
    logger.debug("topicName=" + topicName + ", numEvents=" + numb1)
    logger.trace("Creating Kafka Producer...")

   /*Property Configuration*/

    val props = new Properties()
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "HelloProducer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[Integer,String](props)
    logger.trace("Start sending messages...")

    val i=0
    try{
    for(i<-0 to numb1) {
      val record = new ProducerRecord[Integer, String](topicName,i, "value"+i)
      producer.send(record)
    }
    }
    catch {
      case foo: KafkaException => logger.error("Exception occurred – Check log for more details.\n")
    }
  finally{
    producer.close()
  }
  }

  }

