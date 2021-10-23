package com.jc.user.search.model.config

import com.jc.auth.JwtConfig
import pureconfig.ConfigReader.Result
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException
import pureconfig.generic.semiauto._

final case class AppConfig(
  elasticsearch: ElasticsearchConfig,
  kafka: KafkaConfig,
  grpcApi: HttpApiConfig,
  restApi: HttpApiConfig,
  graphqlApi: HttpApiConfig,
  prometheus: PrometheusConfig,
  jwt: JwtConfig)

object AppConfig {
  implicit lazy val appConfigReader = deriveReader[AppConfig]

  def getConfig(): Result[AppConfig] =
    ConfigSource.default.load[AppConfig]
}
