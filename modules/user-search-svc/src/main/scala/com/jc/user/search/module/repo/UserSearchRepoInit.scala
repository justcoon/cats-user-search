package com.jc.user.search.module.repo

import cats.effect.IO
import com.sksamuel.elastic4s.ElasticClient
import org.typelevel.log4cats.slf4j.Slf4jLogger

object UserSearchRepoInit {

  def elasticsearch(indexName: String, elasticClient: ElasticClient): IO[ESRepositoryInitializer] = {
    Slf4jLogger.fromClass[IO](classOf[ESRepositoryInitializer]).map { logger =>
      new ESRepositoryInitializer(indexName, UserSearchRepo.EsUserSearchRepoService.fields, elasticClient, logger)
    }
  }
}
