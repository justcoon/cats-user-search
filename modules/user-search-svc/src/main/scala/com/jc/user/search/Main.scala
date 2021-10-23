package com.jc.user.search

import cats.effect._
import com.jc.auth.JwtAuthenticator
import com.jc.logging.LogbackLoggingSystem
import com.jc.logging.api.LoggingSystemGrpcApi
import com.jc.user.search.model.config.{AppConfig, HttpApiConfig}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import fs2.grpc.syntax.all._
import fs2._
import io.grpc.ServerServiceDefinition
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.InetSocketAddress

object Main extends IOApp.Simple {

  def runGrpcServer(service: ServerServiceDefinition, config: HttpApiConfig, logger: Logger[IO]) = {
    import eu.timepit.refined.auto._
    val a = new InetSocketAddress(config.address, config.port)

    for {
      _ <- logger.info(s"starting grpc server ${a}")
      server <- NettyServerBuilder
        .forAddress(a)
        .addService(service)
        .resource[IO]
        .evalMap(server => IO(server.start()))
        .useForever
    } yield server
  }

  val run = for {
    logger <- Slf4jLogger.create[IO]
    appConfig <- IO.fromEither(AppConfig.getConfig())
    loggingSystem <- LogbackLoggingSystem.create[IO]()
    authenticator = JwtAuthenticator.live[IO](appConfig.jwt)
    liveApiServiceResource = LoggingSystemGrpcApi.liveApiServiceResource[IO](loggingSystem, authenticator)
    _ <- liveApiServiceResource.use(r => runGrpcServer(r, appConfig.grpcApi, logger))
  } yield ()
}
