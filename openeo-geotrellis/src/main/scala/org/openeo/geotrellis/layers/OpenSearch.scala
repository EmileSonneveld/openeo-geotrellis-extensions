package org.openeo.geotrellis.layers

import java.io.IOException
import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util.concurrent.TimeUnit

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses.{Feature, FeatureCollection}
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.Map

object OpenSearch {
  private val logger = LoggerFactory.getLogger(classOf[OpenSearch])
  private val requestCounter = new AtomicLong

  def apply(endpoint: URL) = new OpenSearch(endpoint)
}

class OpenSearch(endpoint: URL) {
  import OpenSearch._

  def getProducts(collectionId: String, from: LocalDate, to: LocalDate, bbox: ProjectedExtent,
                  correlationId: String = "", attributeValues: Map[String, Any] = Map()): Seq[Feature] = {
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = from.atStartOfDay(UTC)
    val end = to.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, start, end, bbox, correlationId, attributeValues)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                  correlationId: String, attributeValues: Map[String, Any]): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, correlationId, attributeValues, startIndex)
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + itemsPerPage)
    }

    from(startIndex = 1)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, correlationId: String): Seq[Feature] =
    getProducts(collectionId, start, end, bbox, correlationId, Map[String, Any]())

  private def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                          correlationId: String, attributeValues: Map[String, Any], startIndex: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(s"$endpoint/products")
      .param("collection", collectionId)
      .param("start", start format ISO_INSTANT)
      .param("end", end format ISO_INSTANT)
      .param("bbox", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortKeys", "title") // paging requires deterministic order
      .param("startIndex", startIndex.toString)
      .param("accessedFrom", "MEP") // get direct access links instead of download urls
      .params(attributeValues.mapValues(_.toString).toSeq)
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getProducts) }
    FeatureCollection.parse(json)
  }

  def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getCollections) }
    FeatureCollection.parse(json).features
  }

  private def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))

  private def clientId(correlationId: String): String = {
    if (correlationId.isEmpty) correlationId
    else {
      val count = requestCounter.getAndIncrement()
      s"${correlationId}_$count"
    }
  }

  private def execute(request: HttpRequest): String = {
    val url = request.urlBuilder(request)
    val response = request.asString

    logger.debug(s"$url returned ${response.code}")

    val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body

    if (json.trim.isEmpty) {
      throw new IOException(s"$url returned an empty body")
    }

    json
  }

  private def withRetries[R](action: => R): R = {
    @tailrec
    def attempt[R](retries: Int, delay: (Long, TimeUnit))(action: => R): R = {
      val (amount, timeUnit) = delay

      def retryable(e: Exception): Boolean = retries > 0 && (e match {
        case h: HttpStatusException if h.code >= 500 => true
        case e: IOException if e.getMessage.endsWith("returned an empty body") =>
          logger.warn(s"encountered empty body: retrying within $amount $timeUnit", e)
          true
        case _ => false
      })

      try
        return action
      catch {
        case e: Exception if retryable(e) => Unit
      }

      timeUnit.sleep(amount)
      attempt(retries - 1, (amount * 2, timeUnit)) { action }
    }

    attempt(retries = 4, delay = (5, SECONDS)) { action }
  }
}