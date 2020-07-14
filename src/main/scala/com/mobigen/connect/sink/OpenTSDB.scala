package com.mobigen.connect.sink

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

class OpenTSDB {
  def putOpenTSDB[T](ip : String, metric : String, value : String, tags : String, httpClient : CloseableHttpClient, timestamp : Long, currentTime : Long): Unit = {

    val topic = metric.split('.')
    val openTSDBUrl = "http://" + ip + "/" + topic.head + "/_doc/" + currentTime
    val post = new HttpPost(openTSDBUrl)
    val c1 = System.currentTimeMillis() / 1000
    val body1 = f"""{
                   |        "metric": "$metric",
                   |        "timestamp": $timestamp,
                   |        "value": $value,
                   |        "tags": $tags
                   |}""".stripMargin

    println(openTSDBUrl)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(body1))
    println(body1)
    httpClient.execute(post)
  }
}
