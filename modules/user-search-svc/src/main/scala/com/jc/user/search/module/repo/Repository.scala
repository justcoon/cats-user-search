package com.jc.user.search.module.repo

import cats.effect.IO
import com.sksamuel.elastic4s.ElasticClient
import io.circe.{Decoder, Encoder}
import org.typelevel.log4cats.Logger

import scala.reflect.ClassTag

trait Repository[F[_], ID, E <: Repository.Entity[ID]] {

  def insert(value: E): F[Boolean]

  def update(value: E): F[Boolean]

  def delete(id: ID): F[Boolean]

  def find(id: ID): F[Option[E]]

  def findAll(): F[Seq[E]]
}

object Repository {

  trait Entity[ID] {
    def id: ID
  }
}

trait SearchRepository[F[_], E <: Repository.Entity[_]] {

  def search(
    query: Option[String],
    page: Int,
    pageSize: Int,
    sorts: Iterable[SearchRepository.FieldSort]
  ): F[SearchRepository.PaginatedSequence[E]]

  def suggest(
    query: String
  ): F[SearchRepository.SuggestResponse]
}

object SearchRepository {

  final case class FieldSort(property: String, asc: Boolean)

  final case class PaginatedSequence[E](items: Seq[E], page: Int, pageSize: Int, count: Int)

  final case class SuggestResponse(items: Seq[PropertySuggestions])

  final case class TermSuggestion(text: String, score: Double, freq: Int)

  final case class PropertySuggestions(property: String, suggestions: Seq[TermSuggestion])

}

class ESRepository[ID: Encoder: Decoder, E <: Repository.Entity[ID]: Encoder: Decoder: ClassTag](
  indexName: String,
  elasticClient: ElasticClient,
  logger: Logger[IO])
    extends Repository[IO, ID, E] {

  import com.sksamuel.elastic4s.cats.effect.instances._
  import com.sksamuel.elastic4s.ElasticDsl.{search => searchIndex, _}
  import com.sksamuel.elastic4s.circe._
  import cats.syntax.all._

  val serviceLogger = logger

  override def insert(value: E): IO[Boolean] = {
    val id = value.id.toString
    serviceLogger.debug(s"insert - ${indexName} - id: ${id}") *>
      elasticClient.execute {
        indexInto(indexName).doc(value).id(id)
      }.map(_.isSuccess).onError { e =>
        serviceLogger.error(s"insert - ${indexName} - id: ${value.id} - error: ${e.getMessage}")
      }
  }

  override def update(value: E): IO[Boolean] = {
    val id = value.id.toString
    serviceLogger.debug(s"update - ${indexName} - id: ${id}") *>
      elasticClient.execute {
        updateById(indexName, value.id.toString).doc(value)
      }.map(_.isSuccess).onError { e =>
        serviceLogger.error(s"update - ${indexName} - id: ${id} - error: ${e.getMessage}")
      }
  }

  override def delete(id: ID): IO[Boolean] = {
    serviceLogger.debug(s"update - id: ${id}") *>
      elasticClient.execute {
        deleteById(indexName, id.toString)
      }.map(_.isSuccess).onError { e =>
        serviceLogger.error(s"update - id: ${id} - error: ${e.getMessage}")
      }
  }

  override def find(id: ID): IO[Option[E]] = {
    serviceLogger.debug(s"find - ${indexName} - id: ${id}") *>
      elasticClient.execute {
        get(indexName, id.toString)
      }.map { r =>
        if (r.result.exists)
          Option(r.result.to[E])
        else
          Option.empty
      }.onError { e =>
        serviceLogger.error(s"find - ${indexName} - id: ${id} - error: ${e.getMessage}")
      }
  }

  override def findAll(): IO[Seq[E]] = {
    serviceLogger.debug(s"findAll - ${indexName}") *>
      elasticClient.execute {
        searchIndex(indexName).matchAllQuery()
      }.map(_.result.to[E]).onError { e =>
        serviceLogger.error(s"findAll - ${indexName} - error: ${e.getMessage}")
      }
  }
}

class ESSearchRepository[E <: Repository.Entity[_]: Encoder: Decoder: ClassTag](
  indexName: String,
  suggestProperties: Seq[String],
  elasticClient: ElasticClient,
  logger: Logger[IO]
) extends SearchRepository[IO, E] {

  import com.sksamuel.elastic4s.cats.effect.instances._
  import com.sksamuel.elastic4s.ElasticDsl.{search => searchIndex, _}
  import com.sksamuel.elastic4s.requests.searches.queries.QueryStringQuery
  import com.sksamuel.elastic4s.requests.searches.queries.matches.MatchAllQuery
  import com.sksamuel.elastic4s.requests.searches.sort.{FieldSort, SortOrder}
  import com.sksamuel.elastic4s.requests.searches.suggestion
  import com.sksamuel.elastic4s.circe._

  val serviceLogger = logger

  def search(
    query: com.sksamuel.elastic4s.requests.searches.queries.Query,
    page: Int,
    pageSize: Int,
    sorts: Iterable[com.sksamuel.elastic4s.requests.searches.sort.FieldSort])
    : IO[SearchRepository.PaginatedSequence[E]] = {

    val elasticQuery = searchIndex(indexName).query(query).from(page * pageSize).limit(pageSize).sortBy(sorts)

    val q = elasticClient.show(elasticQuery)

    serviceLogger.debug(s"search - ${indexName} - query: '${q}'") *>
      elasticClient.execute {
        elasticQuery
      }.flatMap { res =>
        if (res.isSuccess) {
          IO {
            val items = res.result.to[E]
            SearchRepository.PaginatedSequence(items, page, pageSize, res.result.totalHits.toInt)
          }
        } else {
          IO.raiseError(new Exception(ElasticUtils.getReason(res.error)))
        }
      }.onError { e =>
        serviceLogger.error(s"search - ${indexName} - query: '${q}' - error: ${e.getMessage}")
      }
  }

  override def search(
    query: Option[String],
    page: Int,
    pageSize: Int,
    sorts: Iterable[SearchRepository.FieldSort]): IO[SearchRepository.PaginatedSequence[E]] = {
    val q = query.map(QueryStringQuery(_)).getOrElse(MatchAllQuery())
    val ss = sorts.map { case SearchRepository.FieldSort(property, asc) =>
      val o = if (asc) SortOrder.Asc else SortOrder.Desc
      FieldSort(property, order = o)
    }
    serviceLogger.debug(s"search - ${indexName} - query: '${query
      .getOrElse("N/A")}', page: $page, pageSize: $pageSize, sorts: ${sorts.mkString("[", ",", "]")}") *>
      elasticClient.execute {
        searchIndex(indexName).query(q).from(page * pageSize).limit(pageSize).sortBy(ss)
      }.flatMap { res =>
        if (res.isSuccess) {
          IO {
            val items = res.result.to[E]
            SearchRepository.PaginatedSequence(items, page, pageSize, res.result.totalHits.toInt)
          }
        } else {
          IO.raiseError(new Exception(ElasticUtils.getReason(res.error)))
        }
      }.onError { e =>
        serviceLogger.error(
          s"search - ${indexName} - query: '${query.getOrElse("N/A")}', page: $page, pageSize: $pageSize, sorts: ${sorts
            .mkString("[", ",", "]")} - error: ${e.getMessage}")
      }
  }

  override def suggest(query: String): IO[SearchRepository.SuggestResponse] = {
    // completion suggestion
    val complSuggestions = suggestProperties.map { p =>
      suggestion
        .CompletionSuggestion(ElasticUtils.getSuggestPropertyName(p), ElasticUtils.getSuggestPropertyName(p))
        .prefix(query)
    }

    serviceLogger.debug(s"suggest - ${indexName} - query: '$query'") *>
      elasticClient.execute {
        searchIndex(indexName).suggestions(complSuggestions)
      }.flatMap { res =>
        if (res.isSuccess) {
          val elasticSuggestions = res.result.suggestions
          val suggestions = suggestProperties.map { p =>
            val propertySuggestions = elasticSuggestions(ElasticUtils.getSuggestPropertyName(p))
            val suggestions = propertySuggestions.flatMap { v =>
              val r = v.toCompletion
              r.options.map { o => SearchRepository.TermSuggestion(o.text, o.score, o.score.toInt) }
            }

            SearchRepository.PropertySuggestions(p, suggestions)
          }
          IO(SearchRepository.SuggestResponse(suggestions))
        } else {
          IO.raiseError(new Exception(ElasticUtils.getReason(res.error)))
        }
      }.onError { e =>
        serviceLogger.error(s"suggest - ${indexName} - query: '$query' - error: ${e.getMessage}")
      }
  }
}
