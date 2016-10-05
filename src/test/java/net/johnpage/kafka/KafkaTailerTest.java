package net.johnpage.kafka;

import static org.junit.Assert.assertEquals;

import net.johnpage.kafka.mock.MockKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaTailerTest {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTailerTest.class);
  MockKafkaProducer mockKafkaProducer = null;
  @Test
  public void testMain() throws IOException {
    LOGGER.info("Starting...");
    Properties producerProperties = new Properties();
    try {
      producerProperties.load(new FileInputStream("src/test/resources/KafkaTailer.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    MockKafkaProducer mockKafkaProducer = new MockKafkaProducer(producerProperties);
    ProducerFactory.setInstance(mockKafkaProducer);
    String[] argumentArray = {"src/test/resources/KafkaTailer.txt","src/test/resources/KafkaTailer.properties","test-topic"};
    KafkaTailer.main(argumentArray);
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    KafkaTailer.getTailer().stop();
    String valueOne = mockKafkaProducer.properties.getProperty("propertyOne");
    assertEquals("valueOne",valueOne);
    ProducerRecord producerRecord = mockKafkaProducer.recordList.get(0);
    assertEquals("lineOne",producerRecord.value());
  }
}
