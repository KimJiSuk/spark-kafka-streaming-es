package com.mobigen.connect.sink

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient

class OpenTSDB extends Serializable {
  def putOpenTSDB[T](ip : String, metric : String, value : String, tags : String, httpClient : CloseableHttpClient, timestamp : Long): Unit = {

    val openTSDBUrl = "http://" + ip + "/api/put"
    val post = new HttpPost(openTSDBUrl)
    val c1 = System.currentTimeMillis() / 1000
    val body1 =
      f"""{
         |        "metric": "$metric",
         |        "timestamp": $timestamp,
         |        "value": $value,
         |        "tags": $tags

         |}""".stripMargin

    println(openTSDBUrl)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(body1))
    println(body1)
    var r = httpClient.execute(post)
    println(r)
  }
}
