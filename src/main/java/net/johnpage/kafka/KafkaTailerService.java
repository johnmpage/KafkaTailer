package net.johnpage.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTailerService {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTailerService.class);
  private static KafkaTailerService serviceInstance = new KafkaTailerService();
  public static void windowsService(String args[]) {
    LOGGER.info("Starting...\nargs={}",args);
    String cmd = "start";
    if(args.length > 0) {
      cmd = args[0];
    }
    if("start".equals(cmd)) {
      serviceInstance.start(args);
    }
    else {
      serviceInstance.stop();
    }
  }
  private boolean stopped = false;
  public void start(String args[]) {
    LOGGER.info("Starting...");
    stopped = false;
    System.out.println("Kafka Tailer Service Started " + new java.util.Date());
    KafkaTailer.main(args);
    System.out.println("Kafka Tailer Service Finished " + new java.util.Date());
  }
  public void stop() {
    stopped = true;
    LOGGER.info("Stopping...");
    KafkaTailer.stop();
    synchronized(this) {
      this.notify();
    }
  }
}

