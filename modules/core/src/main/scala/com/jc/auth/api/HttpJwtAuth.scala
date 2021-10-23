package com.jc.auth.api

import cats.effect.kernel.Sync
import com.jc.auth.JwtAuthenticator
//import org.http4s.{Headers, Status}
//import org.typelevel.ci.CIString

object HttpJwtAuth {

//  private val AuthHeader = CIString(JwtAuthenticator.AuthHeader)
//
//  def authenticated[F[_]: Sync](headers: Headers, authenticator: JwtAuthenticator.Service[F]): F[Option[String]] = {
//    import cats.syntax.all._
//    import cats.data.OptionT
//
//    val maybeSubject = for {
//      rawToken <- OptionT(Sync[F].delay(headers.get(AuthHeader).map(_.head)))
//      subject <- OptionT(authenticator.authenticated(JwtAuthenticator.sanitizeBearerAuthToken(rawToken)))
//    } yield subject
//
//    maybeSubject.value
//  }

}
