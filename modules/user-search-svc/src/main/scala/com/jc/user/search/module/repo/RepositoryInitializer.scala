package com.jc.user.search.module.repo

import cats.effect.IO
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.fields.ElasticField
import org.typelevel.log4cats.Logger

trait RepositoryInitializer[F[_]] {
  def init(): F[Boolean]
}

class ESRepositoryInitializer(
  indexName: String,
  fields: Seq[ElasticField],
  elasticClient: ElasticClient,
  logger: Logger[IO])
    extends RepositoryInitializer[IO] {
  import com.sksamuel.elastic4s.ElasticDsl._
  import com.sksamuel.elastic4s.cats.effect.instances._

  override def init(): IO[Boolean] = {
    for {
      existResp <- elasticClient.execute {
        indexExists(indexName)
      }
      initResp <-
        if (!existResp.result.exists) {
          logger.debug(s"init - $indexName - initializing ...") *>
            elasticClient.execute {
              createIndex(indexName).mapping(properties(fields))
            }.map(r => r.result.acknowledged).onError { e =>
              logger.error(s"init: $indexName - error: ${e.getMessage}")
            }
        } else {
          logger.debug(s"init - $indexName - updating ...") *>
            elasticClient.execute {
              putMapping(indexName).properties(fields)
            }.map(r => r.result.acknowledged).onError { e =>
              logger.error(s"init - $indexName - error: ${e.getMessage}")
            }
        }
    } yield initResp
  }
}
