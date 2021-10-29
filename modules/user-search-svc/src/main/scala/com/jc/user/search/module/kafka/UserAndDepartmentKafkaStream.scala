package com.jc.user.search.module.kafka

import com.jc.user.domain.DepartmentEntity.{DepartmentId, DepartmentIdTag}
import com.jc.user.domain.UserEntity.{UserId, UserIdTag}
import com.jc.user.domain.proto.{DepartmentPayloadEvent, UserPayloadEvent}
import com.jc.user.search.model.config.KafkaConfig
import org.apache.kafka.streams.StreamsConfig
import eu.timepit.refined.auto._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import shapeless.tag.@@
import shapeless.tag

import java.util.Properties

object UserAndDepartmentKafkaStream {

  private def taggedStringSerde[T]: Serde[String @@ T] = {
    val serializer = (v: String @@ T) => Serdes.stringSerde.serializer().serialize("", v)
    val deserializer = (b: Array[Byte]) =>
      Option(Serdes.stringSerde.deserializer().deserialize("", b)).map(tag[T][String])
    Serdes.fromFn[String @@ T](serializer, deserializer)
  }

  private implicit val userIdSerde: Serde[UserId] = taggedStringSerde[UserIdTag]

  private implicit val userSerde: Serde[UserPayloadEvent] = {
    val serializer = (v: UserPayloadEvent) => v.toByteArray
    val deserializer = (b: Array[Byte]) => Some(UserPayloadEvent.parseFrom(b))
    Serdes.fromFn(serializer, deserializer)
  }

  private implicit val departmentIdSerde: Serde[DepartmentId] = taggedStringSerde[DepartmentIdTag]

  private implicit val departmentSerde: Serde[DepartmentPayloadEvent] = {
    val serializer = (v: DepartmentPayloadEvent) => v.toByteArray
    val deserializer = (b: Array[Byte]) => Some(DepartmentPayloadEvent.parseFrom(b))
    Serdes.fromFn(serializer, deserializer)
  }

  def streamProperties(config: KafkaConfig): Properties = {

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-search-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.addresses.mkString(","))
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props
  }

  def streamTopology(config: KafkaConfig) = {
    val builder = new StreamsBuilder()

    val usersStream = builder.stream[UserId, UserPayloadEvent](config.userTopic)

//    val departmentsStream = builder.stream[DepartmentId, DepartmentPayloadEvent](config.departmentTopic)
//
//    usersStream.join(departmentsStream)
    usersStream
  }
}
