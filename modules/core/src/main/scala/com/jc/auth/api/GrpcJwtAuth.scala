package com.jc.auth.api

import cats.effect.kernel.Sync
import com.jc.auth.JwtAuthenticator
import io.grpc.{Status, StatusException}

object GrpcJwtAuth {

  import io.grpc.Metadata

  private val AuthHeader: Metadata.Key[String] =
    Metadata.Key.of(JwtAuthenticator.AuthHeader, Metadata.ASCII_STRING_MARSHALLER)

  private val UnauthenticatedException: StatusException = new StatusException(Status.UNAUTHENTICATED)

  def authenticated[F[_]: Sync](ctx: Metadata, authenticator: JwtAuthenticator.Service[F]): F[String] = {
    import cats.syntax.all._
    for {
      rawToken <- Sync[F].fromOption(Option(ctx.get(AuthHeader)), UnauthenticatedException)
      maybeSubject <- authenticator.authenticated(JwtAuthenticator.sanitizeBearerAuthToken(rawToken))
      subject <- Sync[F].fromOption(maybeSubject, UnauthenticatedException)
    } yield subject
  }
}
