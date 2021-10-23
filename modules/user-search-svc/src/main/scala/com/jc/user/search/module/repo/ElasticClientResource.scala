package com.jc.user.search.module.repo

import cats.effect.IO
import cats.effect.kernel.Resource
import com.jc.user.search.model.config.ElasticsearchConfig
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient

object ElasticClientResource {

  def createElasticClient(config: ElasticsearchConfig): Resource[IO, ElasticClient] = {
    import eu.timepit.refined.auto._
    Resource.make {
      IO {
        val prop = ElasticProperties(config.addresses.mkString(","))
        val jc = JavaClient(prop)
        ElasticClient(jc)
      }
    }(c => IO(c.close()))
  }
}
