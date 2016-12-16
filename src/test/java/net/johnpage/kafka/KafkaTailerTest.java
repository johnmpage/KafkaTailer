package net.johnpage.kafka;

import static org.junit.Assert.assertEquals;

import net.johnpage.kafka.mock.MockKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class KafkaTailerTest {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTailerTest.class);
  MockKafkaProducer mockKafkaProducer = null;
  @Test
  public void testMonitorFile() throws IOException {
    LOGGER.info("Starting...");
    Properties producerProperties = new Properties();
    try {
      producerProperties.load(new FileInputStream("src/test/resources/KafkaTailer.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    MockKafkaProducer mockKafkaProducer = new MockKafkaProducer(producerProperties);
    ProducerFactory.setInstance(mockKafkaProducer);
    String[] argumentArray = {"filePath=src/test/resources/KafkaTailer.txt","producerPropertiesPath=src/test/resources/KafkaTailer.properties","kafkaTopic=test-topic","startTailingFrom=end"};
    KafkaTailer.main(argumentArray);
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    String valueOne = mockKafkaProducer.properties.getProperty("propertyOne");
    assertEquals("valueOne",valueOne);
    ProducerRecord producerRecord = mockKafkaProducer.recordList.get(0);
    assertEquals("lineOne",producerRecord.value());
  }
  public static final String VALUE__LINE_ONE="lineOne";
  public static final String VALUE__LINE_TWO="lineTwo";
  public static final String VALUE__LINE_THREE="lineThree";
  public static final String VALUE__LINE_FOUR="lineFour";
  public static final String VALUE__LINE_FIVE="lineFive";
  public static final String VALUE__LINE_SIX="lineSix";
  public static final String VALUE__LINE_SEVEN="lineSeven";
  public static final String VALUE__LINE_EIGHT="lineEight";
  public static final String VALUE__LINE_NINE="lineNine";
  @Test
  public void testMonitorDirectory() throws IOException {
    LOGGER.info("Starting...");
    File directory = new File("temp");
    directory.mkdir();
    File file1 = new File(directory,"test1.log");
    file1.createNewFile();
    FileWriter fileWriter1 = new FileWriter(file1);
    fileWriter1.append(VALUE__LINE_ONE).append('\n');
    fileWriter1.append(VALUE__LINE_TWO).append('\n');
    fileWriter1.append(VALUE__LINE_THREE).append('\n');
    fileWriter1.close();
    LOGGER.info("Completed Setup.");
    Properties producerProperties = new Properties();
    try {
      producerProperties.load(new FileInputStream("src/test/resources/KafkaTailer.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    MockKafkaProducer mockKafkaProducer = new MockKafkaProducer(producerProperties);
    ProducerFactory.setInstance(mockKafkaProducer);
    final String[] argumentArray = {"directoryPath="+directory.getAbsolutePath(),"producerPropertiesPath=src/test/resources/KafkaTailer.properties","kafkaTopic=test-topic","startTailingFrom=start","relinquishLock=true"};
    new Thread() {
      public void run() {
        KafkaTailer.main(argumentArray);
      }
    }.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    LOGGER.info("Records: {}",mockKafkaProducer.recordList);;
    LOGGER.info("Writing second file...");
    File file2 = new File(directory,"test2.log");
    file2.createNewFile();
    FileWriter fileWriter2 = new FileWriter(file2);
    fileWriter2.append(VALUE__LINE_FOUR).append('\n');
    fileWriter2.append(VALUE__LINE_FIVE).append('\n');
    fileWriter2.append(VALUE__LINE_SIX).append('\n');
    fileWriter2.close();
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    LOGGER.info("Records: {}",mockKafkaProducer.recordList);;
    LOGGER.info("Writing third file...");
    File file3 = new File(directory,"test3.log");
    file3.createNewFile();
    FileWriter fileWriter3 = new FileWriter(file3);
    fileWriter3.append(VALUE__LINE_SEVEN).append('\n');
    fileWriter3.append(VALUE__LINE_EIGHT).append('\n');
    fileWriter3.append(VALUE__LINE_NINE).append('\n');
    fileWriter3.close();
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    KafkaTailer.stop();
    // Cleanup
    file1.delete();
    file2.delete();
    file3.delete();
    directory.delete();
    LOGGER.info("Records: {}",mockKafkaProducer.recordList);
    assertEquals(9,mockKafkaProducer.recordList.size());
    assertEquals(VALUE__LINE_ONE,mockKafkaProducer.recordList.get(0).value());
    assertEquals(VALUE__LINE_TWO,mockKafkaProducer.recordList.get(1).value());
    assertEquals(VALUE__LINE_THREE,mockKafkaProducer.recordList.get(2).value());
    assertEquals(VALUE__LINE_FOUR,mockKafkaProducer.recordList.get(3).value());
    assertEquals(VALUE__LINE_FIVE,mockKafkaProducer.recordList.get(4).value());
    assertEquals(VALUE__LINE_SIX,mockKafkaProducer.recordList.get(5).value());
    assertEquals(VALUE__LINE_SEVEN,mockKafkaProducer.recordList.get(6).value());
    assertEquals(VALUE__LINE_EIGHT,mockKafkaProducer.recordList.get(7).value());
    assertEquals(VALUE__LINE_NINE,mockKafkaProducer.recordList.get(8).value());
  }
}
