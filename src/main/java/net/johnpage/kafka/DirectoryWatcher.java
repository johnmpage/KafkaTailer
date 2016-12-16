package net.johnpage.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class DirectoryWatcher {
  private final static Logger LOGGER = LoggerFactory.getLogger(DirectoryWatcher.class);
  private String directoryPathString;
  WatchService watchService;
  TailerThreadManager tailerThreadManager;
  public DirectoryWatcher(String directoryPathString, TailerThreadManager tailerThreadManager) throws IOException {
    LOGGER.debug("directoryPathString={}",directoryPathString);
    this.directoryPathString  = directoryPathString;
    this.tailerThreadManager = tailerThreadManager;
  }
  public void startWatching() {
    LOGGER.debug("Watching: {}",directoryPathString);
    Path directoryPath = Paths.get(directoryPathString);
    WatchKey watchKey=null;
    try {
      watchService  = FileSystems.getDefault().newWatchService();
      watchKey = directoryPath.register(watchService, ENTRY_CREATE);
    } catch (IOException x) {
      LOGGER.error("IOException setting up startWatching.",x);
      System.err.println(x);
    }
    while (true ) {
      try {
        LOGGER.debug("Taking key from WatchService: {}",watchService.toString());
        watchService.take();
      } catch (InterruptedException x) {
        LOGGER.debug("InterruptedException...",x);
        break;
      }catch (Throwable t) {
        LOGGER.error("Throwable...",t);
        break;
      }
      LOGGER.debug("Reviewing Events...");
      for (WatchEvent<?> event : watchKey.pollEvents()) {
        LOGGER.debug("Event: {} : {}",event.kind(),event.context());
        WatchEvent.Kind<?> kind = event.kind();
        if (kind == StandardWatchEventKinds.OVERFLOW) {
          continue;
        }
        if (kind != StandardWatchEventKinds.ENTRY_CREATE) {
          continue;
        }
        WatchEvent<Path> watchEvent = (WatchEvent<Path>) event;
        Path filename = watchEvent.context();
        try {
          Path directoryFilePath = directoryPath.resolve(filename);
          LOGGER.debug("directoryFilePath={}",directoryFilePath);
          tailerThreadManager.startTailingFile(directoryFilePath.toString());
        } catch (Exception x) {
          LOGGER.error("Exception handling new file:"+filename,x);
          System.err.println(x);
          continue;
        }
      }
      LOGGER.debug("Resetting watchKey...");
      boolean valid = watchKey.reset();
      if (!valid) {
        LOGGER.debug("WatchKey is not valid: valid={}",valid);
        break;
      }
    }
  }
}
