package com.jc.user.search.module.processor

import cats.effect.kernel.Async
import com.jc.user.domain.proto.{DepartmentPayloadEvent, UserPayloadEvent}
import com.jc.user.search.module.repo.{DepartmentSearchRepo, UserSearchRepo}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import org.typelevel.log4cats.slf4j.Slf4jLogger

object EventProcessor {

  sealed trait EventEnvelope[E] {
    def topic: String
    def event: E
  }

  object EventEnvelope {
    final case class User(topic: String, event: UserPayloadEvent) extends EventEnvelope[UserPayloadEvent]

    final case class Department(topic: String, event: DepartmentPayloadEvent)
        extends EventEnvelope[DepartmentPayloadEvent]

    final case class Unknown(topic: String, event: Array[Byte]) extends EventEnvelope[Array[Byte]]
  }

  trait Service[F[_]] {
    def process(eventEnvelope: EventEnvelope[_]): F[Boolean]
  }

  final case class LiveEventProcessorService[F[_]: Async](
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F],
    logger: Logger[F])
      extends Service[F] {

    override def process(eventEnvelope: EventEnvelope[_]): F[Boolean] = {
      eventEnvelope match {
        case EventEnvelope.User(topic, event) =>
          logger.debug(
            s"processing event - topic: ${topic}, entityId: ${event.entityId}, type: ${event.payload.getClass.getSimpleName}") *>
            processUser(event, userSearchRepo, departmentSearchRepo)
        case EventEnvelope.Department(topic, event) =>
          logger.debug(
            s"processing event - topic: ${topic}, entityId: ${event.entityId}, type: ${event.payload.getClass.getSimpleName}") *>
            processDepartment(event, userSearchRepo, departmentSearchRepo)
        case e: EventEnvelope[_] =>
          logger.debug(
            s"processing event - topic: ${e.topic}, type: ${e.event.getClass.getSimpleName}) - unknown event, skipping") *>
            Async[F].delay(false)
      }
    }
  }

  def live[F[_]: Async](
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F]): F[LiveEventProcessorService[F]] = {
    Slf4jLogger.fromClass[F](classOf[LiveEventProcessorService[F]]).map { logger =>
      LiveEventProcessorService(userSearchRepo, departmentSearchRepo, logger)
    }
  }

  def processUser[F[_]: Async](
    event: UserPayloadEvent,
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F]): F[Boolean] = {
    if (isUserRemoved(event))
      userSearchRepo.delete(event.entityId)
    else
      userSearchRepo
        .find(event.entityId)
        .flatMap {
          case u @ Some(_) => getUpdatedUser(event, u, departmentSearchRepo).flatMap(userSearchRepo.update)
          case None => getUpdatedUser(event, None, departmentSearchRepo).flatMap(userSearchRepo.insert)
        }
  }

  def isUserRemoved(event: UserPayloadEvent): Boolean = {
    import com.jc.user.domain.proto._
    event match {
      case UserPayloadEvent(_, _, _: UserPayloadEvent.Payload.Removed, _) => true
      case _ => false
    }
  }

  def createUser(id: com.jc.user.domain.UserEntity.UserId): UserSearchRepo.User = UserSearchRepo.User(id, "", "", "")

  def getUpdatedUser[F[_]: Async](
    event: UserPayloadEvent,
    user: Option[UserSearchRepo.User],
    departmentSearchRepo: DepartmentSearchRepo.Service[F]): F[UserSearchRepo.User] = {
    import io.scalaland.chimney.dsl._
    import com.jc.user.domain.proto._
    import UserSearchRepo.User._

    val currentUser = user.getOrElse(createUser(event.entityId))

    def getDepartment(d: Option[com.jc.user.domain.proto.DepartmentRef]): F[Option[UserSearchRepo.Department]] = {
      d match {
        case Some(d) => departmentSearchRepo.find(d.id).map(_.map(_.transformInto[UserSearchRepo.Department]))
        case _ => Async[F].delay(None)
      }
    }

    event match {
      case UserPayloadEvent(_, _, payload: UserPayloadEvent.Payload.Created, _) =>
        val na = payload.value.address.map(_.transformInto[UserSearchRepo.Address])
        getDepartment(payload.value.department).map { nd =>
          usernameEmailPassAddressDepartmentLens.set(currentUser)(
            (
              payload.value.username,
              payload.value.email,
              payload.value.pass,
              na,
              nd
            ))
        }

      case UserPayloadEvent(_, _, payload: UserPayloadEvent.Payload.PasswordUpdated, _) =>
        Async[F].delay(passLens.set(currentUser)(payload.value.pass))

      case UserPayloadEvent(_, _, payload: UserPayloadEvent.Payload.EmailUpdated, _) =>
        Async[F].delay(emailLens.set(currentUser)(payload.value.email))

      case UserPayloadEvent(_, _, payload: UserPayloadEvent.Payload.AddressUpdated, _) =>
        val na = payload.value.address.map(_.transformInto[UserSearchRepo.Address])
        Async[F].delay(addressLens.set(currentUser)(na))

      case UserPayloadEvent(_, _, payload: UserPayloadEvent.Payload.DepartmentUpdated, _) =>
        getDepartment(payload.value.department).map { nd => departmentLens.set(currentUser)(nd) }

      case _ => Async[F].delay(currentUser)
    }
  }

  def processDepartment[F[_]: Async](
    event: DepartmentPayloadEvent,
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F]): F[Boolean] = {
    if (isDepartmentRemoved(event))
      departmentSearchRepo.delete(event.entityId)
    else
      departmentSearchRepo
        .find(event.entityId)
        .flatMap {
          case d @ Some(_) =>
            for {
              dep <- getUpdatedDepartment(event, d)
              res <- departmentSearchRepo.update(dep)
              _ <- updateUsersDepartment(dep, userSearchRepo)
            } yield res
          case None => getUpdatedDepartment(event, None).flatMap(departmentSearchRepo.insert)
        }
  }

  def updateUsersDepartment[F[_]: Async](
    department: DepartmentSearchRepo.Department,
    userSearchRepo: UserSearchRepo.Service[F]): F[Long] = {

    import io.scalaland.chimney.dsl._
    import UserSearchRepo.User._

    val pageInitial = 0
    val pageSize = 20

    val userDepartment = Some(department.transformInto[UserSearchRepo.Department])

    fs2.Stream
      .unfoldEval(pageInitial) { page =>
        userSearchRepo
          .searchByDepartment(department.id, page, pageSize)
          .map { r =>
            if ((r.page * r.pageSize) < r.count || r.items.nonEmpty) Some((r.items, r.page + 1))
            else None
          }
      }
      .flatMap(fs2.Stream.emits)
      .map(user => departmentLens.set(user)(userDepartment))
      .mapAsync(1)(userSearchRepo.update)
      .compile
      .count
  }

  def isDepartmentRemoved(event: DepartmentPayloadEvent): Boolean = {
    import com.jc.user.domain.proto._
    event match {
      case DepartmentPayloadEvent(_, _, _: DepartmentPayloadEvent.Payload.Removed, _) => true
      case _ => false
    }
  }

  def createDepartment(id: com.jc.user.domain.DepartmentEntity.DepartmentId): DepartmentSearchRepo.Department =
    DepartmentSearchRepo.Department(id, "", "")

  def getUpdatedDepartment[F[_]: Async](
    event: DepartmentPayloadEvent,
    user: Option[DepartmentSearchRepo.Department]): F[DepartmentSearchRepo.Department] = {
    import com.jc.user.domain.proto._
    import DepartmentSearchRepo.Department._

    val currentDepartment = user.getOrElse(createDepartment(event.entityId))

    event match {
      case DepartmentPayloadEvent(_, _, payload: DepartmentPayloadEvent.Payload.Created, _) =>
        Async[F].delay(nameDescriptionLens.set(currentDepartment)((payload.value.name, payload.value.description)))
      case DepartmentPayloadEvent(_, _, payload: DepartmentPayloadEvent.Payload.Updated, _) =>
        Async[F].delay(nameDescriptionLens.set(currentDepartment)((payload.value.name, payload.value.description)))
      case _ => Async[F].delay(currentDepartment)
    }
  }
}
