package com.jc.user.search.module.kafka

import cats.effect.IO
import com.jc.user.domain.proto.{DepartmentPayloadEvent, UserPayloadEvent}
import com.jc.user.search.model.config.KafkaConfig
import com.jc.user.search.module.processor.EventProcessor
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, ConsumerSettings, Deserializer}
import fs2.kafka
import eu.timepit.refined.auto._
import scala.concurrent.duration._

object KafkaConsumer {

  def consumerSettings(config: KafkaConfig): ConsumerSettings[IO, String, EventProcessor.EventEnvelope[_]] = {
    ConsumerSettings(Deserializer.string[IO], eventDes(config))
      .withGroupId(s"user-search-${config.userTopic}-2")
      .withClientId("user-search-client-2")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
  }

  def eventDes(config: KafkaConfig): Deserializer[IO, EventProcessor.EventEnvelope[_]] = {
    Deserializer.topic { topic =>
      if (topic == config.userTopic.value) {
        Deserializer.lift { data =>
          IO(EventProcessor.EventEnvelope.User(topic, UserPayloadEvent.parseFrom(data)))
        }
      } else if (topic == config.departmentTopic.value) {
        Deserializer.lift { data =>
          IO(EventProcessor.EventEnvelope.Department(topic, DepartmentPayloadEvent.parseFrom(data)))
        }
      } else
        Deserializer.lift { data =>
          IO(EventProcessor.EventEnvelope.Unknown(topic, data))
        }
    }
  }

  def consume(config: KafkaConfig, processor: EventProcessor.Service[IO]) = {
    kafka.KafkaConsumer
      .stream(consumerSettings(config))
      .subscribeTo(config.userTopic, config.departmentTopic)
      .records
      .mapAsync(1) { cr =>
        processor.process(cr.record.value).as(cr.offset)
      }
      .through(commitBatchWithin(10, 15.seconds))
  }

}
