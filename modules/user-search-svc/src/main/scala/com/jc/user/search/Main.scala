package com.jc.user.search

import cats.effect._
import cats.effect.kernel.Resource
import com.jc.auth.JwtAuthenticator
import com.jc.logging.LogbackLoggingSystem
import com.jc.logging.api.LoggingSystemGrpcApi
import com.jc.user.search.model.config.{AppConfig, ElasticsearchConfig, HttpApiConfig}
import com.jc.user.search.module.api.UserSearchGrpcApi
import com.jc.user.search.module.kafka.KafkaConsumer
import com.jc.user.search.module.processor.EventProcessor
import com.jc.user.search.module.repo.{
  DepartmentSearchRepo,
  DepartmentSearchRepoInit,
  UserSearchRepo,
  UserSearchRepoInit
}
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.ServerServiceDefinition
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import eu.timepit.refined.auto._

import java.net.InetSocketAddress

object Main extends IOApp.Simple {

  def runGrpcServer(service: List[ServerServiceDefinition], config: HttpApiConfig, logger: Logger[IO]) = {
    import fs2.grpc.syntax.all._
    import scala.jdk.CollectionConverters._
    val a = new InetSocketAddress(config.address, config.port)
    for {
      _ <- logger.info(s"starting grpc server ${a}")
      server <- NettyServerBuilder
        .forAddress(a)
        .addServices(service.asJava)
        .resource[IO]
        .evalMap(server => IO(server.start()))
        .useForever
    } yield server
  }

  def createElasticClient(config: ElasticsearchConfig): Resource[IO, ElasticClient] = {
    Resource.make {
      IO {
        val prop = ElasticProperties(config.addresses.mkString(","))
        val jc = JavaClient(prop)
        ElasticClient(jc)
      }
    }(c => IO(c.close()))
  }

  val run = for {
    logger <- Slf4jLogger.create[IO]
    appConfig <- IO.fromEither(AppConfig.getConfig())
    loggingSystem <- LogbackLoggingSystem.create[IO]()
    authenticator = JwtAuthenticator.live[IO](appConfig.jwt)

    resources = for {
      elasticClient <- createElasticClient(appConfig.elasticsearch)
      _ <- Resource.eval(UserSearchRepoInit.elasticsearchInit(appConfig.elasticsearch.userIndexName, elasticClient))
      _ <- Resource.eval(
        DepartmentSearchRepoInit.elasticsearchInit(appConfig.elasticsearch.departmentIndexName, elasticClient))
      userRepo <- Resource.eval(UserSearchRepo.elasticsearch(appConfig.elasticsearch.userIndexName, elasticClient))
      depRepo <- Resource.eval(
        DepartmentSearchRepo.elasticsearch(appConfig.elasticsearch.departmentIndexName, elasticClient))
      eventProcessor <- Resource.eval(EventProcessor.live[IO](userRepo, depRepo))
      userSearchGrpcApi <- UserSearchGrpcApi.liveApiServiceResource[IO](userRepo, depRepo, authenticator)
      loggingSystemGrpcApi <- LoggingSystemGrpcApi.liveApiServiceResource[IO](loggingSystem, authenticator)
    } yield {
      (eventProcessor, userSearchGrpcApi :: loggingSystemGrpcApi :: Nil)
    }
    _ <- resources.use { case (eventProcessor, grpcApiServices) =>
      KafkaConsumer.consume(appConfig.kafka, eventProcessor).compile.drain &>
        runGrpcServer(grpcApiServices, appConfig.grpcApi, logger)
    }
  } yield ()
}
