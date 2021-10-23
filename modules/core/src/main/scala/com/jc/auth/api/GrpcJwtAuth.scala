package com.jc.auth.api

import cats.effect.kernel.Sync
import com.jc.auth.JwtAuthenticator
import io.grpc.{Status, StatusException}

object GrpcJwtAuth {

  import io.grpc.Metadata

  private val AuthHeader: Metadata.Key[String] =
    Metadata.Key.of(JwtAuthenticator.AuthHeader, Metadata.ASCII_STRING_MARSHALLER)

  def authenticated[F[_]: Sync](ctx: Metadata, authenticator: JwtAuthenticator.Service[F]): F[String] = {
    import cats.syntax.all._
    import cats.data.OptionT

    val maybeSubject = for {
      rawToken <- OptionT(Sync[F].delay(Option(ctx.get(AuthHeader))))
      subject <- OptionT(authenticator.authenticated(JwtAuthenticator.sanitizeBearerAuthToken(rawToken)))
    } yield subject

    maybeSubject.value.flatMap {
      case Some(s) => Sync[F].delay(s)
      case None => Sync[F].raiseError(new StatusException(Status.UNAUTHENTICATED))
    }
  }
}
