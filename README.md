# Kafka Tailer

## Tail the files in any directory and streams to a Kafka topic. 

Usage:

```
java -classpath KafkaTailer-2.0-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer directoryPath=C:\\iis-logs\\W3SVC1\\ producerPropertiesPath=C:\\iis-logs\\kafka-producer.properties kafkaTopic=a-topic
```

### New in Version 2.0
* Watch a log **directory**, not a single file. Bridge IIS to Kafka!
* Unordered, name-based invocation arguments

This is a **JVM-based** tail that is integrated with a Kafka Producer.  
It posts lines to a remote Kafka queue as they are added to a local file.  
KafkaTailer can operates much like the `tail` command. Useful when outside of the *nix world. 



### How do I stream logs from IIS? 
Using KafkaTailer you can stream logs from Microsoft's IIS server to a remote Kafka topic.
IIS simply needs to have logging enabled in a dedicated directory. KafkaTailer will monitor the logging directory, detect new logging files as they are generated(Hourly, Daily, or Weekly), and send the latest log out across the network.

### JVM-based.
* Works on all OSs
* KafkaTailer uses the reference Kafka Producer written by the core Kafka team.

### Kafka Producer Properties File
Because KafkaTailer gives you full access to every Producer configuration parameter as the developers intended;  it provides you with the full power of the latest Kafka Producer.  A typical Kafka Producer properties file might read:

```properties
bootstrap.servers=a.domain.com:9092
value.serializer=org.apache.kafka.common.serialization.StringSerializer
key.serializer=org.apache.kafka.common.serialization.StringSerializer
security.protocol=SSL
ssl.truststore.location=a.kafka.client.truststore.jks
ssl.truststore.password=apassword
```

A complete reference to the producer properties is [here](https://kafka.apache.org/documentation.html#producerconfigs).

###Command-line Arguments
* **directoryPath** or **filePath**: Required. One of these parameters must be present.
* **producerPropertiesPath** : Required.
* **kafkaTopic** : Required.
* **startTailingFrom** : Optional. A value of "beginning" will Start sending lines from the beginning of the file. The default behavior is to start from the end of the file only sending new lines as they are added.
* **relinquishLock** : Optional. A value of "true" will relinquish the lock on a monitored file between file reads. 

The order of the arguments is not important. Here is an example of usage with the optional arguments:

```
java -classpath KafkaTailer-2.0-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer directoryPath=C:\\iis-logs\\W3SVC1\\ producerPropertiesPath=C:\\iis-logs\\kafka-producer.properties kafkaTopic=a-topic startTailingFrom=beginning relinquishLock=true
```

##Tailing a Single File

```
java -classpath KafkaTailer-2.0-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer filePath=C:\\logs\\a.log producerPropertiesPath=C:\\kafka\\kafka-producer.properties kafkaTopic=a-topic relinquishLock=true
```

###A note on Log Rotation
Typically, software applications rotate logs in one of two styles:

1. Application (ie. Apache HTTPD) always writes to a single file:
* The base log file is copied over to a new, dated file.
* The base log file is not deleted or moved. The content within it is removed, leaving it blank.
* New log statements are written to the same file.

2. Application (ie. IIS) generates a new file with each log rotation:
* A new file is created
* New log statements are written to the new log file.

The log rotation mechanism of your application should be reviewed and or tested to be certain that KafkaTailer will work continuously for you. 
Currently, if KafkaTailer attempts to read a file and it does not exist, KafkaTailer will quietly shutdown. 

### Built using:
* [Apache Commons IO Tailer 2.5](https://commons.apache.org/proper/commons-io/)
* [Apache Kafka Producer 0.10](https://kafka.apache.org/)

### Kafka Version
Tested with Kafka 0.10. Should be backwards compatible with 0.90 and 0.82. These 3 versions rely on the following initialization of the Producer:

```java
new KafkaProducer(Properties properties) 
```

Version-appropriate properties will need to be used.

### Building
You may choose to build KafkaTailer yourself, either to embed a different version of the Kafka client libraries or for security reasons. If you you choose to build KafkaTailer yourself, a jar with it's dependencies embedded requires a special Maven invocation.

```
mvn compile assembly:single
```


