package com.jc.logging.api

import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import com.jc.auth.JwtAuthenticator
import com.jc.auth.api.GrpcJwtAuth
import com.jc.logging.LoggingSystem
import com.jc.logging.proto.{
  GetLoggerConfigurationReq,
  GetLoggerConfigurationsReq,
  LogLevel,
  LoggerConfiguration,
  LoggerConfigurationRes,
  LoggerConfigurationsRes,
  LoggingSystemApiServiceFs2Grpc,
  SetLoggerConfigurationReq
}
import io.grpc.{Metadata, ServerServiceDefinition}

object LoggingSystemGrpcApi {

  /** using [[LoggingSystem.LogLevelMapping]] for api <-> logging system
    *  level mapping
    *
    * it is specially handled like: log level not defined
    */
  private val logLevelMapping: LoggingSystem.LogLevelMapping[LogLevel] = LoggingSystem.LogLevelMapping(
    Seq(
      (LoggingSystem.LogLevel.TRACE, LogLevel.TRACE),
      (LoggingSystem.LogLevel.DEBUG, LogLevel.DEBUG),
      (LoggingSystem.LogLevel.INFO, LogLevel.INFO),
      (LoggingSystem.LogLevel.WARN, LogLevel.WARN),
      (LoggingSystem.LogLevel.ERROR, LogLevel.ERROR),
      (LoggingSystem.LogLevel.FATAL, LogLevel.FATAL),
      (LoggingSystem.LogLevel.OFF, LogLevel.OFF)
    )
  )

  final case class LiveLoggingSystemGrpcService[F[_]: Sync](
    loggingSystem: LoggingSystem.Service[F],
    authenticator: JwtAuthenticator.Service[F])
      extends LoggingSystemApiServiceFs2Grpc[F, io.grpc.Metadata] {
    import cats.syntax.all._

    def getSupportedLogLevels: F[Seq[LogLevel]] =
      loggingSystem.getSupportedLogLevels.map { levels =>
        levels.map(logLevelMapping.toLogger).toSeq
      }

    def toApiLoggerConfiguration(configuration: LoggingSystem.LoggerConfiguration): LoggerConfiguration =
      LoggerConfiguration(
        configuration.name,
        logLevelMapping.toLogger(configuration.effectiveLevel),
        configuration.configuredLevel.flatMap(logLevelMapping.toLogger.get)
      )

    override def setLoggerConfiguration(
      request: SetLoggerConfigurationReq,
      ctx: Metadata): F[LoggerConfigurationRes] = {
      for {
        _ <- GrpcJwtAuth.authenticated(ctx, authenticator)
        res <- loggingSystem.setLogLevel(request.name, request.level.flatMap(logLevelMapping.fromLogger.get))
        levels <- getSupportedLogLevels
        configuration <-
          if (res) {
            loggingSystem.getLoggerConfiguration(request.name)
          } else Sync[F].delay(None)
      } yield LoggerConfigurationRes(configuration.map(toApiLoggerConfiguration), levels)
    }

    override def getLoggerConfiguration(
      request: GetLoggerConfigurationReq,
      ctx: Metadata): F[LoggerConfigurationRes] = {
      for {
        _ <- GrpcJwtAuth.authenticated(ctx, authenticator)
        levels <- getSupportedLogLevels
        configuration <- loggingSystem.getLoggerConfiguration(request.name)
      } yield LoggerConfigurationRes(configuration.map(toApiLoggerConfiguration), levels)
    }

    override def getLoggerConfigurations(
      request: GetLoggerConfigurationsReq,
      ctx: Metadata): F[LoggerConfigurationsRes] = {
      for {
        _ <- GrpcJwtAuth.authenticated(ctx, authenticator)
        levels <- getSupportedLogLevels
        configurations <- loggingSystem.getLoggerConfigurations
      } yield LoggerConfigurationsRes(configurations.map(toApiLoggerConfiguration), levels)
    }
  }

  def liveApiServiceResource[F[_]: Async](
    loggingSystem: LoggingSystem.Service[F],
    authenticator: JwtAuthenticator.Service[F]): Resource[F, ServerServiceDefinition] =
    LoggingSystemApiServiceFs2Grpc.bindServiceResource(
      new LiveLoggingSystemGrpcService[F](loggingSystem, authenticator))
}
