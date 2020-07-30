package com.mobigen.connect.sink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.http.impl.client.HttpClientBuilder
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers._
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import io.confluent.kafka.schemaregistry.client._
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType


object SparkConsumer {

  def main(args: Array[String]): Unit = {

    // Load Properties
    val props = AppConfig.loadProperties()

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(props.getProperty("appname"))
      .getOrCreate()

    def createStreamingContext(): StreamingContext = {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

      // Set Kafka Info
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> props.getProperty("broker_list"),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[KafkaAvroDeserializer],
        "group.id" -> props.getProperty("consumer_group"),
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "schema.registry.url" -> props.getProperty("schema_registry")
      )

      val topics = Array(props.getProperty("topic_list"))

      val stream = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, GenericRecord](topics, kafkaParams)
      )

      // Get Kafka Data
      val l2Avro = stream.map(record => record.value)

      /*
      Set Schema Registry Info
       */
      val schemaRegistry = new CachedSchemaRegistryClient(props.getProperty("schema_registry"), 1000)
      val m = schemaRegistry.getLatestSchemaMetadata(props.getProperty("schema_registry_subject"))
      val schemaId = m.getId
      val schema = schemaRegistry.getById(schemaId)

      var nameList = List[String]()
      var typeList = List[String]()

      for ( w <- 0 to (schema.getFields.size() - 1)) {
        nameList = nameList ::: List[String](schema.getFields.get(w).name())
        typeList = typeList ::: List[String](schema.getFields.get(w).schema().getType.getName)
      }

      /*
      Kafka Data Processing
       */
      l2Avro.foreachRDD({
        rdd =>
          println("rdd count : " + rdd.count().toString)
          if(rdd.count() >= 1) {
            val l2Obj : RDD[Row] = rdd.map(
              v => {
                var tempList = List[Any]()

                for (i <- 0 to nameList.size - 1) {
                  if (typeList(i).equals("string")) {
                    tempList = tempList ::: List[Any](v.get(nameList(i)).toString)
                  }
                  else {
                    tempList = tempList ::: List[Any](v.get(nameList(i)))
                  }
                }

                Row.fromSeq(tempList)
              })

            val schemaStructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

            val l2Raws : DataFrame = spark.createDataFrame(l2Obj, schemaStructType)
            var timestamp = new Date()

            l2Raws.show()

            l2Raws.foreach(k => {
              val ip = props.getProperty("elasticsearch")
              val topic = props.getProperty("topic_list")
              var tags = "{"
              k.schema.distinct.foreach(field => {
                if (field.name.startsWith("tg_")) {
                  tags += "\"" + field.name.substring(3, field.name.length) + "\": \"" + k.getAs(field.name).toString + "\","
                }
                else if (field.name.equals("_timestamp")) {
                  val _timestamp = k.getAs(field.name).toString
                  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  timestamp = format.parse(_timestamp)
                }
              })

              tags = (tags.substring(0, tags.length - 1) + "}").stripMargin

              /*
              ElasticSearch example
               */
              if (timestamp != 0L) {
                var metricJSON = "{"
                k.schema.distinct.foreach(field => {
                  val metric = topic + "." + field.name.substring(2, field.name.length)
                  val value = if (field.dataType.typeName.equals("string")) "1" else k.getAs(field.name).toString
                  if (field.name.startsWith("m_")) {
                    metricJSON += f""" "$metric": "$value",""".stripMargin
                  }
                })
                metricJSON = (metricJSON.substring(0, metricJSON.length - 1) + "}").stripMargin

                val currentDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(timestamp)

                val httpClient = HttpClientBuilder.create().build()
                new ElasticSearch().putElasticSearch(ip, topic, metricJSON, tags, httpClient, currentDate.toString)
                httpClient.close()
              }

              /*
              OpenTSDB example
               */
//              if (timestamp != 0L) {
//                k.schema.distinct.foreach(field => {
//                  if (field.name.startsWith("m_")) {
//                    val httpClient = HttpClientBuilder.create().build()
//                    val metric = topic + "." + field.name.substring(2, field.name.length)
//                    val value = if (field.dataType.typeName.equals("string")) "1" else k.getAs(field.name).toString
//                    new OpenTSDB().putOpenTSDB(ip, metric, value, tags, httpClient, timestamp)
//                    httpClient.close()
//                  }
//                })
//
//              }

            })

          }
      })

      ssc
    }

    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach {
        _.stop(stopSparkContext = false)
      }
    }

    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
