package com.jc.user.search.module.api

import cats.effect.Resource
import cats.effect.kernel.Async
import com.jc.auth.JwtAuthenticator
import com.jc.auth.api.GrpcJwtAuth
import com.jc.user.domain.proto
import com.jc.user.domain.proto.{Department, User}
import com.jc.user.search.api.proto.{
  FieldSort,
  GetDepartmentReq,
  GetDepartmentRes,
  GetUserReq,
  GetUserRes,
  PropertySuggestion,
  SearchDepartmentStreamReq,
  SearchDepartmentsReq,
  SearchDepartmentsRes,
  SearchUserStreamReq,
  SearchUsersReq,
  SearchUsersRes,
  SuggestDepartmentsReq,
  SuggestDepartmentsRes,
  SuggestUsersReq,
  SuggestUsersRes,
  UserSearchApiServiceFs2Grpc
}
import com.jc.user.search.module.repo.{DepartmentSearchRepo, SearchRepository, UserSearchRepo}
import io.grpc.{Metadata, ServerServiceDefinition}

object UserSearchGrpcApi {

  def toRepoFieldSort(sort: FieldSort): SearchRepository.FieldSort = {
    SearchRepository.FieldSort(sort.field, sort.order.isAsc)
  }

  final case class LiveUserSearchGrpcApiService[F[_]: Async](
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F],
    authenticator: JwtAuthenticator.Service[F])
      extends UserSearchApiServiceFs2Grpc[F, io.grpc.Metadata] {
    import io.scalaland.chimney.dsl._
    import cats.syntax.all._

    override def getUser(request: GetUserReq, ctx: Metadata): F[GetUserRes] = {
      for {
        _ <- GrpcJwtAuth.authenticated(ctx, authenticator)
        res <- userSearchRepo.find(request.id)
      } yield GetUserRes(res.map(_.transformInto[proto.User]))
    }

    override def searchUsers(request: SearchUsersReq, ctx: Metadata): F[SearchUsersRes] = {
      val ss = request.sorts.map(toRepoFieldSort)
      val q = if (request.query.isBlank) None else Some(request.query)
      userSearchRepo
        .search(q, request.page, request.pageSize, ss)
        .onError { e =>
          Async[F].delay(SearchUsersRes(result = SearchUsersRes.Result.Failure(e.getMessage)))
        }
        .map { r =>
          SearchUsersRes(
            r.items.map(_.transformInto[proto.User]),
            r.page,
            r.pageSize,
            r.count,
            SearchUsersRes.Result.Success("")
          )
        }
    }

    override def searchUserStream(request: SearchUserStreamReq, ctx: Metadata): fs2.Stream[F, User] = {
      val ss = request.sorts.map(toRepoFieldSort)
      val q = if (request.query.isBlank) None else Some(request.query)

      val pageInitial = 0
      val pageSize = 20

      fs2.Stream
        .unfoldEval(pageInitial) { page =>
          userSearchRepo
            .search(q, page, pageSize, ss)
            .map { r =>
              if ((r.page * r.pageSize) < r.count || r.items.nonEmpty) Some((r.items, r.page + 1))
              else None
            }
        }
        .flatMap(fs2.Stream.emits)
        .map(_.transformInto[proto.User])
    }

    override def suggestUsers(request: SuggestUsersReq, ctx: Metadata): F[SuggestUsersRes] = {
      userSearchRepo
        .suggest(request.query)
        .onError { e =>
          Async[F].delay(SearchUsersRes(result = SearchUsersRes.Result.Failure(e.getMessage)))
        }
        .map { r =>
          SuggestUsersRes(r.items.map(_.transformInto[PropertySuggestion]), SuggestUsersRes.Result.Success(""))
        }
    }

    override def getDepartment(request: GetDepartmentReq, ctx: Metadata): F[GetDepartmentRes] = {
      for {
        _ <- GrpcJwtAuth.authenticated(ctx, authenticator)
        res <- departmentSearchRepo.find(request.id)
      } yield GetDepartmentRes(res.map(_.transformInto[proto.Department]))
    }

    override def searchDepartments(request: SearchDepartmentsReq, ctx: Metadata): F[SearchDepartmentsRes] = {
      val ss = request.sorts.map(toRepoFieldSort)
      val q = if (request.query.isBlank) None else Some(request.query)
      departmentSearchRepo
        .search(q, request.page, request.pageSize, ss)
        .onError { e =>
          Async[F].delay(SearchDepartmentsRes(result = SearchDepartmentsRes.Result.Failure(e.getMessage)))
        }
        .map { r =>
          SearchDepartmentsRes(
            r.items.map(_.transformInto[proto.Department]),
            r.page,
            r.pageSize,
            r.count,
            SearchDepartmentsRes.Result.Success(""))
        }
    }

    override def searchDepartmentStream(
      request: SearchDepartmentStreamReq,
      ctx: Metadata): fs2.Stream[F, Department] = {
      val ss = request.sorts.map(toRepoFieldSort)
      val q = if (request.query.isBlank) None else Some(request.query)

      val pageInitial = 0
      val pageSize = 20

      fs2.Stream
        .unfoldEval(pageInitial) { page =>
          departmentSearchRepo
            .search(q, page, pageSize, ss)
            .map { r =>
              if ((r.page * r.pageSize) < r.count || r.items.nonEmpty) Some((r.items, r.page + 1))
              else None
            }
        }
        .flatMap(fs2.Stream.emits)
        .map(_.transformInto[proto.Department])
    }

    override def suggestDepartments(request: SuggestDepartmentsReq, ctx: Metadata): F[SuggestDepartmentsRes] = {
      departmentSearchRepo
        .suggest(request.query)
        .onError { e =>
          Async[F].delay(SearchDepartmentsRes(result = SearchDepartmentsRes.Result.Failure(e.getMessage)))
        }
        .map { r =>
          SuggestDepartmentsRes(
            r.items.map(_.transformInto[PropertySuggestion]),
            SuggestDepartmentsRes.Result.Success(""))
        }
    }
  }

  def liveApiServiceResource[F[_]: Async](
    userSearchRepo: UserSearchRepo.Service[F],
    departmentSearchRepo: DepartmentSearchRepo.Service[F],
    authenticator: JwtAuthenticator.Service[F]): Resource[F, ServerServiceDefinition] =
    UserSearchApiServiceFs2Grpc.bindServiceResource(
      new LiveUserSearchGrpcApiService[F](userSearchRepo, departmentSearchRepo, authenticator))
}
