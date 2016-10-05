package net.johnpage.kafka;

import org.apache.commons.io.input.Tailer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class KafkaTailer {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTailer.class);
  private static Tailer tailer = null;
  public static void main(String args[]) {
    LOGGER.debug("Starting... args[]={}",args);
    System.out.println("KafkaTailer: Starting...");
    try {
      String filePath = args[0];
      String producerPropertiesPath = args[1];
      String kafkaTopic = args[2];
      if(filePath==null || filePath.length()<1 || producerPropertiesPath==null || producerPropertiesPath.length()<1 || kafkaTopic==null || kafkaTopic.length()<1 ){
        printUsageInstructions();
        return;
      }
      File file = new File(filePath);
      System.out.println("KafkaTailer: File = "+file.getAbsolutePath());
      File propertiesFile =new File(producerPropertiesPath);
      System.out.println("KafkaTailer: Producer Properties File = "+propertiesFile.getAbsolutePath());
      System.out.println("KafkaTailer: Kafka Topic = "+ kafkaTopic);
      Connector connector = new Connector();
      Properties producerProperties = new Properties();
      producerProperties.load(new FileInputStream(producerPropertiesPath));
      ProducerFactory.setProperties(producerProperties);
      Producer producer = ProducerFactory.getInstance();
      connector.setProducer(producer);
      connector.setTopic(kafkaTopic);
      tailer = new Tailer(file, connector, 1000);
      Thread thread = new Thread(tailer);
      //thread.setDaemon(true);
      Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){tailer.stop();}});
      thread.start();
    } catch (FileNotFoundException e) {
      LOGGER.error("FileNotFoundException.",e);
      e.printStackTrace();
    } catch (IOException e) {
      LOGGER.error("IOException.",e);
      e.printStackTrace();
    }catch (Exception e) {
      LOGGER.error("Exception.",e);
      e.printStackTrace();
    }
  }
  protected static Tailer getTailer() {
    return tailer;
  }
  private static void printUsageInstructions(){
    System.out.println("Usage: java -classpath KafkaTailer-0.1-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer a-log.log kafka-producer.properties a-topic");
  }
}
