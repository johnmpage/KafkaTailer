package net.johnpage.kafka;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailerThreadManager extends TailerListenerAdapter {
  private final static Logger LOGGER = LoggerFactory.getLogger(TailerThreadManager.class);
  private Tailer currentTailer;
  private String currentFilePath;
  private TailerFactory tailorFactory;
  private boolean shutdownHookAdded = false;
  public TailerThreadManager(TailerFactory tailorFactory) {
    this.tailorFactory = tailorFactory;
  }
  public void startTailingFile(String newfilePath) {
    LOGGER.debug("newfilePath={}",newfilePath);
    if(newfilePath!=null && !newfilePath.equals(currentFilePath)){
      if(currentTailer!=null){
        System.out.println("KafkaTailer: No longer tailing file: "+currentFilePath);
        LOGGER.info("No longer tailing file: "+currentFilePath);
        currentTailer.stop();
      }
      currentTailer = tailorFactory.getNewInstance(newfilePath);
      Thread thread = new Thread(currentTailer);
      addShutdownHookOnce();
      thread.start();
      currentFilePath = newfilePath;
      System.out.println("KafkaTailer: Now tailing file: "+currentFilePath);
      LOGGER.info("Now tailing file: "+currentFilePath);
    }
  }
  private synchronized void addShutdownHookOnce(){
    if( !shutdownHookAdded ) {
      final TailerThreadManager thisReference = this;
      Runtime.getRuntime().addShutdownHook(
        new Thread() {
          public void run() {
            thisReference.shutdown();
          }
        }
      );
      LOGGER.debug("Shutdown Hook Added.");
      shutdownHookAdded = true;
    }
  }
  public void shutdown() {
    LOGGER.info("Shutting down...");
    if (currentTailer != null) {
      currentTailer.stop();
      LOGGER.info("No longer tailing file: " + currentFilePath);
    } else {
      LOGGER.info("No tailer currently exists.");
    }
  }
}
