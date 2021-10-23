package com.jc.user.search.model.config

import com.jc.auth.JwtConfig
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

  def getConfig(): Either[ConfigReaderException[AppConfig], AppConfig] =
    ConfigSource.default.load[AppConfig].left.map(failures => ConfigReaderException(failures))
}
