package net.johnpage.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerFactory{
  private final static Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);
  private static Producer producer;
  private static Properties properties;
  public static void setProperties(Properties properties) {
    ProducerFactory.properties = properties;
  }
  public static Producer getInstance() {
    LOGGER.debug("producer="+producer);
    if(producer==null){
      LOGGER.debug("Producer is null. Initializing a KafkaProducer using stored properties: {}",ProducerFactory.properties);
      producer = new KafkaProducer(ProducerFactory.properties);
    }
    return producer;
  }
  protected static void setInstance(Producer thisProducer) {
    LOGGER.debug("Setting producer: {}",producer);
    producer = thisProducer;
  }
}
