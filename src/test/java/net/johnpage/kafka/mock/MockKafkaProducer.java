package net.johnpage.kafka.mock;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class MockKafkaProducer implements Producer {
  public Properties properties;
  public List<ProducerRecord> recordList = new ArrayList<ProducerRecord>();
  public MockKafkaProducer(Properties properties) {
    this.properties = properties;
  }
  public Future<RecordMetadata> send(ProducerRecord producerRecord) {
    recordList.add(producerRecord);
    return null;
  }
  public Future<RecordMetadata> send(ProducerRecord producerRecord, Callback callback) {
    recordList.add(producerRecord);
    return null;
  }
  public void flush() {}
  public List<PartitionInfo> partitionsFor(String s) {
    return null;
  }
  public Map<MetricName, ? extends Metric> metrics() {
    return null;
  }
  public void close() {}
  public void close(long l, TimeUnit timeUnit) {}
  @Override
  public String toString() {
    return "MockKafkaProducer{" +
      "properties=" + properties +
      ", recordList=" + recordList +
      '}';
  }
}
