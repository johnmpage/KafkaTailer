package net.johnpage.kafka;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TailerFactory {
  private final static Logger LOGGER = LoggerFactory.getLogger(TailerFactory.class);
  private static boolean startTailingFromEnd = true;
  private static boolean relinquishLockBetweenChunks = false;
  private static int delayInMillisecondsBetweenChecks = 1000;
  private static TailerListener listener;
  public static void setStartTailingFromEnd(boolean startTailingFromEnd) {
    TailerFactory.startTailingFromEnd = startTailingFromEnd;
  }
  public static void setRelinquishLockBetweenChunks(boolean relinquishLockBetweenChunks) {
    TailerFactory.relinquishLockBetweenChunks = relinquishLockBetweenChunks;
  }
  public void setDelayInMillisecondsBetweenChecks(int delayInMillisecondsBetweenChecks) {
    TailerFactory.delayInMillisecondsBetweenChecks = delayInMillisecondsBetweenChecks;
  }
  public static void setListener(TailerListener listener) {
    TailerFactory.listener = listener;
  }
  public static Tailer getNewInstance(String filePath){
    LOGGER.debug("Adding new Tailer to file: {}. Adding listener:{} ",filePath,listener);
    File file = new File(filePath);
    return new Tailer(file, listener, delayInMillisecondsBetweenChecks, startTailingFromEnd, relinquishLockBetweenChunks);
  }
}
