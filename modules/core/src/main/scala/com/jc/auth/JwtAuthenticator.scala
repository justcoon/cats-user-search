package com.jc.auth

import cats.effect.kernel.Sync

import java.time.Clock
import io.circe.{Decoder, Encoder}
import pdi.jwt._

import scala.util.Try

object JwtAuthenticator {
  val AuthHeader = "Authorization"

  val BearerTokenPrefix = "Bearer "

  def sanitizeBearerAuthToken(header: String): String = header.replaceFirst(BearerTokenPrefix, "")

  trait Service[F[_]] {
    def authenticated(rawToken: String): F[Option[String]]
  }

  final case class PdiJwtAuthenticator[F[_]: Sync](helper: PdiJwtHelper, clock: Clock) extends Service[F] {

    override def authenticated(rawToken: String): F[Option[String]] = {
      Sync[F].delay {
        for {
          claim <- helper.decodeClaim(rawToken).toOption
          subject <-
            if (claim.isValid(clock)) {
              claim.subject
            } else None
        } yield subject
      }
    }
  }

  def live[F[_]: Sync](config: JwtConfig): PdiJwtAuthenticator[F] = {
    val helper = new PdiJwtHelper(config)
    PdiJwtAuthenticator(helper, Clock.systemUTC())
  }

}

final class PdiJwtHelper(val config: JwtConfig) {
  import eu.timepit.refined.auto._

  def claim[T: Encoder](
    content: T,
    issuer: Option[String] = None,
    subject: Option[String] = None,
    audience: Option[Set[String]] = None
  )(implicit clock: Clock): JwtClaim = {
    import io.circe.syntax._
    val jsContent = content.asJson.noSpaces

    JwtClaim(
      content = jsContent,
      issuer = issuer.orElse(config.issuer.map(_.value)),
      subject = subject,
      audience = audience).issuedNow
      .expiresIn(config.expiration)
  }

  def encodeClaim(claim: JwtClaim): String =
    JwtCirce.encode(claim, config.secret, PdiJwtHelper.Algorithm)

  def encode[T: Encoder](
    content: T,
    issuer: Option[String] = None,
    subject: Option[String] = None,
    audience: Option[Set[String]] = None
  )(implicit clock: Clock): String =
    encodeClaim(claim(content, issuer, subject, audience))

  def decodeClaim(rawToken: String): Try[JwtClaim] =
    JwtCirce.decode(rawToken, config.secret, Seq(PdiJwtHelper.Algorithm), JwtOptions(expiration = false))

  def decode[T: Decoder](rawToken: String): Try[(JwtClaim, T, String)] =
    for {
      claim <- decodeClaim(rawToken)
      content <- io.circe.parser.decode[T](claim.content).toTry
    } yield (claim, content, rawToken)

}

object PdiJwtHelper {
  val Algorithm = JwtAlgorithm.HS512
}
