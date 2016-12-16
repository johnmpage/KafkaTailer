package net.johnpage.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.InvalidParameterException;
import java.util.Properties;

public class KafkaTailer {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTailer.class);
  public final static String ARG__DIRECTORY_PATH = "directoryPath";
  public final static String ARG__FILE_PATH = "filePath";
  public final static String ARG__PRODUCER_PROPERTIES_PATH = "producerPropertiesPath";
  public final static String ARG__KAFKA_TOPIC = "kafkaTopic";
  public final static String ARG__START_TAILING_FROM = "startTailingFrom";
  public final static String ARG__RELINQUISH_LOCK = "relinquishLock";
  private static final int MODE__DIRECTORY = 1;
  private static final int MODE__FILE = 2;
  private static String  filePath;
  private static String  directoryPath;
  private static String  producerPropertiesPath;
  private static String  kafkaTopic;
  private static boolean startTailingFromEnd = true;
  private static boolean relinquishLockBetweenChunks = false;
  private static int mode;
  public static void main(String args[]) {
    LOGGER.debug("Starting... args[]={}",args);
    System.out.println("KafkaTailer: Starting...");
    try {
      parseArguments(args);
      determineMode();
      TailerThreadManager tailerThreadManager = getTailerThreadManager();
      if(mode==MODE__FILE){
        System.out.println("KafkaTailer: filePath = "+filePath);
        tailerThreadManager.startTailingFile(filePath);
      }else if (mode == MODE__DIRECTORY) {
        final File lastModifiedFile = getLastModifiedFile(directoryPath);
        LOGGER.debug("lastModifiedFile={}",lastModifiedFile.getAbsolutePath());
        tailerThreadManager.startTailingFile(lastModifiedFile.getAbsolutePath());
        LOGGER.debug("directoryPath={}",directoryPath);
        DirectoryWatcher directoryWatcher = new DirectoryWatcher(directoryPath,tailerThreadManager);
        directoryWatcher.startWatching();
      }
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
  private static void parseArguments(String args[]) throws InvalidParameterException {
    filePath = getArgumentValue(ARG__FILE_PATH, args);
    directoryPath = getArgumentValue(ARG__DIRECTORY_PATH, args);
    if( directoryPath!=null && !directoryPath.endsWith(File.separator)){
      directoryPath+=File.separator; // Otherwise the Windows startWatching service will not work.
    }
    LOGGER.debug("directoryPath={}",directoryPath);
    producerPropertiesPath = getArgumentValue(ARG__PRODUCER_PROPERTIES_PATH, args);
    kafkaTopic = getArgumentValue(ARG__KAFKA_TOPIC, args);
    if (
      ((filePath == null || filePath.length() < 1) && (directoryPath == null || directoryPath.length() < 1))
        || producerPropertiesPath == null || producerPropertiesPath.length() < 1 || kafkaTopic == null || kafkaTopic.length() < 1) {
      printUsageInstructions();
      throw new InvalidParameterException("A parameter is missing.");
    }
    String startTailingFromString = getArgumentValue(ARG__START_TAILING_FROM, args);
    startTailingFromEnd = "beginning".equals(startTailingFromString);
    String relinquishLockString = getArgumentValue(ARG__RELINQUISH_LOCK, args);
    relinquishLockBetweenChunks = "true".equals(relinquishLockString);
    File propertiesFile = new File(producerPropertiesPath);
    System.out.println("KafkaTailer: Producer Properties File = "+ propertiesFile.getAbsolutePath());
    System.out.println("KafkaTailer: Kafka Topic = " + kafkaTopic);
  }
  private static String getArgumentValue(String argumentName, String args[]){
    String value = null;
    for( String argument:args ){
      LOGGER.debug("argument={}",argument);
      if(argument.startsWith(argumentName) && argument.length()>argumentName.length()+1){
        value = argument.substring(argumentName.length()+1);
        LOGGER.debug("value={}",value);
        break;
      }
    }
    return value;
  }
  private static void determineMode() {
    if(directoryPath!=null){
      mode = MODE__DIRECTORY;
    }else if(filePath!=null){
      mode = MODE__FILE;
    }
  }
  private static TailerThreadManager getTailerThreadManager() throws IOException {
    TailerFactory tailerFactory = new TailerFactory();
    tailerFactory.setStartTailingFromEnd(startTailingFromEnd);
    tailerFactory.setRelinquishLockBetweenChunks(relinquishLockBetweenChunks);
    tailerFactory.setListener(getConnector());
    TailerThreadManager tailerThreadManager = new TailerThreadManager(tailerFactory);
    return tailerThreadManager;
  }
  private static Connector getConnector() throws IOException {
    Connector connector = new Connector();
    connector.setTopic(kafkaTopic);
    connector.setProducer(getProducer());
    return connector;
  }
  private static Producer getProducer() throws IOException {
    Properties producerProperties = new Properties();
    producerProperties.load(new FileInputStream(producerPropertiesPath));
    ProducerFactory.setProperties(producerProperties);
    Producer producer = ProducerFactory.getInstance();
    return producer;
  }
  private static void printUsageInstructions(){
    System.out.println("Usage: java -classpath KafkaTailer-2.0-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer directoryPath=C:\\\\iis-logs\\\\W3SVC1\\\\ producerPropertiesPath=C:\\\\kafka-producer.properties kafkaTopic=a-topic [startFromBeginning] [relinquishLock]");
  }
  public static File getLastModifiedFile(String directoryPath) {
    File fl = new File(directoryPath);
    File[] files = fl.listFiles(new FileFilter() {
      public boolean accept(File file) {
        return file.isFile();
      }
    });
    long lastModifiedTime = Long.MIN_VALUE;
    File lastModifiedFile = null;
    for (File file : files) {
      if (file.lastModified() > lastModifiedTime) {
        lastModifiedFile = file;
        lastModifiedTime = file.lastModified();
      }
    }
    return lastModifiedFile;
  }
  public static void stop(){
    LOGGER.debug("Stopping...");
  }
}
