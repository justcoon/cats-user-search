package com.jc.user.search.module.repo

import cats.effect.IO
import com.sksamuel.elastic4s.ElasticClient
import org.typelevel.log4cats.slf4j.Slf4jLogger

object DepartmentSearchRepoInit {

  def elasticsearch(indexName: String, elasticClient: ElasticClient): IO[ESRepositoryInitializer] = {
    Slf4jLogger.fromClass[IO](classOf[ESRepositoryInitializer]).map { logger =>
      new ESRepositoryInitializer(
        indexName,
        DepartmentSearchRepo.EsDepartmentSearchRepoService.fields,
        elasticClient,
        logger)
    }
  }

  def elasticsearchInit(indexName: String, elasticClient: ElasticClient): IO[Boolean] = {
    elasticsearch(indexName, elasticClient).flatMap(_.init())
  }
}
