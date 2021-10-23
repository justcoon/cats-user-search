package com.jc.user.search.module.repo

import cats.effect.IO
import com.jc.user.domain.DepartmentEntity.DepartmentId
import com.sksamuel.elastic4s.ElasticClient
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object DepartmentSearchRepo {

  trait Service[F[_]] extends Repository[F, DepartmentId, Department] with SearchRepository[F, Department]

  final case class Department(
    id: DepartmentId,
    name: String,
    description: String
  ) extends Repository.Entity[DepartmentId]

  object Department {
    import shapeless._

    val nameLens: Lens[Department, String] = lens[Department].name
    val descriptionLens: Lens[Department, String] = lens[Department].description

    val nameDescriptionLens: ProductLensBuilder[Department, (String, String)] = nameLens ~ descriptionLens

    import io.circe._, io.circe.generic.semiauto._

    implicit val departmentDecoder: Decoder[Department] = deriveDecoder[Department]

    implicit val departmentEncoder: Encoder[Department] = new Encoder[Department] {

      val derived: Encoder[Department] = deriveEncoder[Department]

      override def apply(a: Department): Json = {
        derived(a).mapObject { jo => jo.add(ElasticUtils.getSuggestPropertyName("name"), Json.fromString(a.name)) }
      }
    }
  }

  final case class EsDepartmentSearchRepoService(indexName: String, elasticClient: ElasticClient, logger: Logger[IO])
      extends DepartmentSearchRepo.Service[IO] {
    private val repo = new ESRepository[DepartmentId, Department](indexName, elasticClient, logger)

    private val searchRepo =
      new ESSearchRepository[Department](
        indexName,
        EsDepartmentSearchRepoService.suggestProperties,
        elasticClient,
        logger)

    override def insert(value: Department): IO[Boolean] = repo.insert(value)

    override def update(value: Department): IO[Boolean] = repo.update(value)

    override def delete(id: DepartmentId): IO[Boolean] = repo.delete(id)

    override def find(id: DepartmentId): IO[Option[Department]] = repo.find(id)

    override def findAll(): IO[Seq[Department]] = repo.findAll()

    override def search(
      query: Option[String],
      page: Int,
      pageSize: Int,
      sorts: Iterable[SearchRepository.FieldSort]): IO[SearchRepository.PaginatedSequence[Department]] =
      searchRepo.search(query, page, pageSize, sorts)

    override def suggest(query: String): IO[SearchRepository.SuggestResponse] =
      searchRepo.suggest(query)
  }

  object EsDepartmentSearchRepoService {
    import com.sksamuel.elastic4s.ElasticDsl._

    val suggestProperties = Seq("name")

    val fields = Seq(
      textField("id").fielddata(true),
      textField("name").fielddata(true),
      textField("description").fielddata(true)
    ) ++ suggestProperties.map(prop => completionField(ElasticUtils.getSuggestPropertyName(prop)))
  }

  def elasticsearch(indexName: String, elasticClient: ElasticClient): IO[EsDepartmentSearchRepoService] = {
    Slf4jLogger.fromClass[IO](classOf[EsDepartmentSearchRepoService]).map { logger =>
      EsDepartmentSearchRepoService(indexName, elasticClient, logger)
    }
  }
}
