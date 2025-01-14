package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.time.{LocalDate, LocalTime, OffsetTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util.{Collections, UUID}
import scala.collection.JavaConverters._

class BatchProcessingApiTest {
  private val endpoint = "https://services.sentinel-hub.com"
  private val batchProcessingApi = new BatchProcessingApi(endpoint)
  private val bucketName = "openeo-sentinelhub"

  private def accessToken: String = new AuthApi().authenticate(Utils.clientId, Utils.clientSecret).access_token

  @Ignore
  @Test
  def createBatchProcess(): Unit = {
    val dateTimes = Seq("2020-11-06T16:50:26Z", "2020-11-06T16:50:26Z", "2020-11-05T05:01:26Z", "2020-11-05T05:01:26Z")
      .map(ZonedDateTime.parse(_, ISO_OFFSET_DATE_TIME))

    val batchProcess = batchProcessingApi.createBatchProcess(
      datasetId = "sentinel-1-grd",
      boundingBox = ProjectedExtent(Extent(586240.0, 5350920.0, 588800.0, 5353480.0), CRS.fromEpsgCode(32633)),
      dateTimes,
      bandNames = Seq("VV", "VH"),
      SampleType.FLOAT32,
      additionalDataFilters = Map("orbitDirection" -> "DESCENDING".asInstanceOf[Any]).asJava,
      processingOptions = Map(
        "backCoeff" -> "GAMMA0_ELLIPSOID",
        "orthorectify" -> false
      ).asJava,
      bucketName,
      description = "BatchProcessingApiTest.createBatchProcess",
      accessToken
    )

    println(batchProcess.id)
  }

  @Test
  def getBatchProcess(): Unit = {
    val batchProcess = batchProcessingApi.getBatchProcess("b330c22b-ec60-4a9a-80bd-ca8e5a246320", accessToken)

    assertEquals("DONE", batchProcess.status)
    assertEquals(Some(BigDecimal("45.77636855174205")), batchProcess.valueEstimate)
    assertEquals(
      Some((ZonedDateTime.parse("2019-10-10T06:06:11Z"), ZonedDateTime.parse("2019-10-10T06:06:15Z"))),
      batchProcess.timeRange)
    assertEquals(4.0 / (24 * 60 * 60), batchProcess.temporalIntervalInDays.get, 0.000000001)
  }

  @Ignore
  @Test
  def startBatchProcess(): Unit = {
    batchProcessingApi.startBatchProcess("479cca6e-53d5-4477-ac5b-2c0ba8d3beba", accessToken)
  }

  @Ignore
  @Test
  def createCard4LBatchProcess(): Unit = {
    val bounds = // the intersection of a feature with the initial bounding box
      """{
        |  "type":"Polygon",
        |  "coordinates":[
        |    [
        |      [
        |        35.715368104951445,
        |        -6.075694
        |      ],
        |      [
        |        35.7316723233983,
        |        -6.1452107540348715
        |      ],
        |      [
        |        35.75107377067866,
        |        -6.23476
        |      ],
        |      [
        |        35.861576,
        |        -6.23476
        |      ],
        |      [
        |        35.861576,
        |        -6.075694
        |      ],
        |      [
        |        35.715368104951445,
        |        -6.075694
        |      ]
        |    ]
        |  ]
        |}
        |""".stripMargin.parseGeoJson[Polygon]()

    val card4lId = UUID.randomUUID()

    val batchProcess = batchProcessingApi.createCard4LBatchProcess(
      datasetId = "sentinel-1-grd",
      bounds,
      dateTime = ZonedDateTime.parse("2021-02-15T15:54:57Z", ISO_OFFSET_DATE_TIME),
      bandNames = Seq("VH", "VV"),
      dataTakeId = "044CD7",
      card4lId,
      demInstance = null,
      additionalDataFilters = Collections.emptyMap[String, Any],
      bucketName,
      subFolder = card4lId.toString,
      accessToken
    )

    println(s"batch process ${batchProcess.id} will write to folder $card4lId")
  }

  @Test(expected = classOf[SentinelHubException])
  def getUnknownBatchProcess(): Unit =
    batchProcessingApi.getBatchProcess("479cca6e-53d5-4477-ac5b-2c0ba8d3bebe", accessToken)

  @Ignore
  @Test
  def createBatchProcessForSparsePolygons(): Unit = {
    val bboxLeft = Extent(3.7614440917968746, 50.737052666897405, 3.7634181976318355, 50.738139065342224)
    val bboxRight = Extent(4.3924713134765625, 50.741235162650355, 4.3979644775390625, 50.74297323282792)

    val polygons = Seq(bboxLeft, bboxRight)
      .map(extent => extent.toPolygon())

    val date = LocalDate.of(2020, 11, 5)
    val startOfDay = date.atStartOfDay(ZoneOffset.UTC)
    val endOfDay = date.atTime(OffsetTime.of(LocalTime.MAX, ZoneOffset.UTC)).toZonedDateTime

    val batchProcess = batchProcessingApi.createBatchProcess(
      datasetId = "sentinel-1-grd",
      geometry = MultiPolygon(polygons),
      geometryCrs = LatLng,
      dateTimes = Seq(startOfDay, endOfDay),
      bandNames = Seq("VV", "VH"),
      SampleType.FLOAT32,
      additionalDataFilters = Collections.emptyMap[String, Any],
      processingOptions = Collections.emptyMap[String, Any],
      bucketName,
      description = "BatchProcessingApiTest.createBatchProcessForSparsePolygons",
      accessToken
    )

    println(batchProcess.id)
  }

  @Test(expected = classOf[SentinelHubException])
  def startJustCreatedBatchProcessIsRetried(): Unit = {
    // mimics a batch process that should have been created but returns a 404 Not Found on /start (it's actually an
    // unknown batch process ID)
    batchProcessingApi.startBatchProcess("18d81bca-eb5a-4dde-8066-0b42b197373e", accessToken)
    // no assertions, visual inspection only (the logger output)
  }
}
