# Kafka Tailer
=========================
## Tail any text file and stream to a Kafka queue. 
Usage:
```
java -classpath KafkaTailer-0.1-jar-with-dependencies.jar net.johnpage.kafka.KafkaTailer a-log.log kafka-producer.properties a-topic
```
This is a **JVM-based** tail that is integrated with a Kafka Producer. It posts lines as they are added to a remote server from your file.  Operates much like the `tail` command. Useful when outside of the *nix world. It does not maintain a lock on the file and allows for log rotations.

### Why would you use this when *nix has `tail`? 
1. Universal
 * Windows
 * Linux
 * Unix
 * MacOS
2. JVM-based.
 * The default Kafka Producer is written for the JVM.
 * Perhaps you already have a JVM running on your server.
3. Standardize your integration 
 * One set of instructions across all platforms. 
 * One jar to rule them all.

### Building
Building a jar with it's dependencies embedded requires a special Maven invocation.
```
mvn compile assembly:single
```

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

### Built using:
 * Apache Commons IO Tailer 2.5
 * Apache Kafka Producer 0.10

### Kafka Version
Are you using a different version of Kafka? Alter the pom dependency and re-build. (Don't forget to make the appropriate changes to your properties file.)
