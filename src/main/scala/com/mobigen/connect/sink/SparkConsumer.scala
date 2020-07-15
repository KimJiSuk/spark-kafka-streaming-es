package com.mobigen.connect.sink

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
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType


object SparkConsumer {

  def main(args: Array[String]): Unit = {

    val props = AppConfig.loadProperties()

    val spark = SparkSession.builder()
      .master("local[2]")
//      .master("spark://" + props.getProperty("spark_master_ip") + ":7077")
      .appName(props.getProperty("appname"))
      .getOrCreate()

    def createStreamingContext(): StreamingContext = {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

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

      val l2Avro = stream.map(record => record.value)

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
            var timestamp = 0L

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
                  timestamp = format.parse(_timestamp).getTime() / 1000
                }
              })

              tags = (tags.substring(0, tags.length - 1) + "}").stripMargin

              if (timestamp != 0L) {
                k.schema.distinct.foreach(field => {
                  if (field.name.startsWith("m_")) {
                    val httpClient = HttpClientBuilder.create().build()
                    val metric = topic + "." + field.name.substring(2, field.name.length)
                    val value = if (field.dataType.typeName.equals("string")) "1" else k.getAs(field.name).toString
                    new ElasticSearch().putElasticSearch(ip, metric, value, tags, httpClient, timestamp)
                    httpClient.close()
                  }
                })

              }

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
