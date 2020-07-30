package com.mobigen.connect.sink

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient

class ElasticSearch extends Serializable {
  def putElasticSearch[T](ip : String, topic : String, metricJSON : String, tags : String, httpClient : CloseableHttpClient, timestamp : String): Unit = {

    val elasticSearchUrl = "http://" + ip + "/" + topic + "/_doc"
    val post = new HttpPost(elasticSearchUrl)
    val body1 = f"""{
                   |        "metric": $metricJSON,
                   |        "@timestamp": "$timestamp",
                   |        "tags": $tags
                   |}""".stripMargin

    println(elasticSearchUrl)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(body1, "UTF-8"))
    println(body1)
    val r = httpClient.execute(post)
    println(r)
  }
}
