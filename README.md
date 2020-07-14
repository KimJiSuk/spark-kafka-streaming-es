# spark-kakfa-streaming

Spark Kafka streaming & Put OpenTSDB



# Package source
```bash
sbt package
```

# Run Spark
```bash
spark-submit \
--jars ./external/kafka-schema-registry-5.2.1.jar,\
./external/kafka-avro-serializer-5.3.0.jar,\
./external/kafka-schema-registry-client-5.3.0.jar,\
./external/common-config-5.3.0.jar,\
./external/spark-avro_2.11-2.4.4.jar,\
./external/common-utils-5.3.0.jar \
--class com.mobigen.connect.sink.SparkConsumer \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4 \
./target/scala-2.11/spark-kafka-streaming_2.11-1.0.1.jar
```


# Check Run Spark(Consumer)

*  Control Center
```bash
http://localhost:9021
```

*  View Dashboard openTSDB
```bash
http://1.255.242.238:3000
```
