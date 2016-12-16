package net.johnpage.kafka;

import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connector extends TailerListenerAdapter {
  private final static Logger LOGGER = LoggerFactory.getLogger(Connector.class);
  Producer producer;
  String topic;
  public void handle(String string) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, string);
    producer.send(producerRecord);
  }
  public void setProducer(Producer producer) {
    this.producer = producer;
  }
  public void setTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String toString() {
    return "Connector{" +
      "producer=" + producer +
      ", topic='" + topic + '\'' +
      '}';
  }
}
