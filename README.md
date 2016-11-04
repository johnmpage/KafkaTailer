# Kafka Tailer

## Tail any text file and stream to a Kafka topic. 

Usage:
```
java -classpath KafkaTailer-1.1-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer a-log.log kafka-producer.properties a-topic
```
This is a **JVM-based** tail that is integrated with a Kafka Producer. It posts lines as they are added to a remote server from your file.  KafkaTailer operates much like the `tail` command. Useful when outside of the *nix world. It does not maintain a lock on the file and allows for log rotations.

### Why would you use this when *nix has `tail`? 
1. Universal
* Windows
* Linux
* Unix
* MacOS
2. JVM-based.
* The reference Kafka Producer by the core Kafka team is written for the JVM.
* Perhaps you already have a JVM running on your server.
3. Standardize your integration 
* One set of instructions across all platforms.


### Kafka Producer Properties File
A typical Kafka Producer properties file might read:
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
* **The path to the text file** : Required.
* **The path to the Kafka Producer properties file** : Required.
* **The Kafka topic** : Required.
* **startFromBeginning** : Optional. Start sending lines from the beginning of the file. The default behavior is to start from the end of the file only sending new lines as they are added to the watched file.
* **relinquishLock** : Optional. Between file reads, relinquish the lock on the file. 

The order of the arguments is important. Here is an example of usage with the optional arguments:

```
java -classpath KafkaTailer-1.1-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer a-log.log kafka-producer.properties a-topic startFromBeginning relinquishLock
```

### Log Rotation
KafkaTailer works with Log Rotation when it is done in what many would call the "proper" way. 
* The base log file is copied to a dated file.
* The log file is not deleted or moved. The content within it is simply removed, leaving it blank.

The log rotation mechanism of your application should be reviewed and or tested to be certain that KafkaTailer will work continuously for you. Currently, if KafkaTailer attempts to read a file and it does not exist, KafkaTailer will quietly shutdown. 

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
Yopu may choose to build KafkaTailer yourself, either to embed a different version of the Kafka client libraries or for security reasons. If you you choose to build KafkaTailer yourself, a jar with it's dependencies embedded requires a special Maven invocation.
```
mvn compile assembly:single
```


