package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.openeo.geotrelliscommon.parseToInclusiveTemporalInterval
import org.openeo.geotrellissentinelhub.SampleType.SampleType

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.ZonedDateTime
import java.util
import java.util.UUID
import scala.collection.JavaConverters._

object BatchProcessingService {
  case class BatchProcess(id: String, status: String, value_estimate: java.math.BigDecimal, errorMessage: String)
}

class BatchProcessingService(endpoint: String, val bucketName: String, authorizer: Authorizer) {
  import BatchProcessingService._

  // convenience methods for Python client
  def this(endpoint: String, bucket_name: String, client_id: String, client_secret: String) =
    this(endpoint, bucket_name,
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(client_id, client_secret))

  def this(endpoint: String, bucket_name: String, client_id: String, client_secret: String,
           zookeeper_connection_string: String, zookeeper_access_token_path: String) =
    this(endpoint, bucket_name,
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(
        zookeeper_connection_string, zookeeper_access_token_path,
        client_id, client_secret))

  private def authorized[R](fn: String => R): R = authorizer.authorized(fn)

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                          from_datetime: String, until_datetime: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process(collection_id, dataset_id, polygons, polygonsCrs, from_datetime, until_datetime, band_names, sampleType,
      metadata_properties, processing_options, subfolder)
  }

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_datetime: String,
                          until_datetime: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any]): String = {
    start_batch_process(collection_id, dataset_id, bbox, bbox_srs, from_datetime, until_datetime, band_names,
      sampleType, metadata_properties, processing_options, subfolder = null)
  }

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_datetime: String, until_datetime: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any]): String =
    start_batch_process(collection_id, dataset_id, polygons, crs, from_datetime, until_datetime, band_names,
      sampleType, metadata_properties, processing_options, subfolder = null)

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_datetime: String, until_datetime: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    // TODO: implement retries
    val (from, to) = parseToInclusiveTemporalInterval(from_datetime, until_datetime)

    val multiPolygon = simplify(polygons)
    val multiPolygonCrs = crs

    val dateTimes = {
      if (from isAfter to) Seq()
      else authorized { accessToken =>
        val catalogApi = if (collection_id == null) new MadeToMeasureCatalogApi else new DefaultCatalogApi(endpoint)
        catalogApi.dateTimes(collection_id, multiPolygon, multiPolygonCrs, from, to,
          accessToken, Criteria.toQueryProperties(metadata_properties, collection_id))
      }
    }

    if (dateTimes.isEmpty)
      throw NoSuchFeaturesException(message =
        s"""no features found for criteria:
           |collection ID "$collection_id"
           |${polygons.length} polygon(s)
           |[$from_datetime, $until_datetime)
           |metadata properties $metadata_properties""".stripMargin)

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    val batchRequestId = authorized { accessToken =>
      batchProcessingApi.createBatchProcess(
        dataset_id,
        multiPolygon,
        multiPolygonCrs,
        dateTimes,
        band_names.asScala,
        sampleType,
        additionalDataFilters = Criteria.toDataFilters(metadata_properties),
        processing_options,
        bucketName,
        description = s"$dataset_id ${polygons.length} $from_datetime ${ISO_OFFSET_DATE_TIME format to} $band_names",
        accessToken,
        subfolder
      ).id
    }

    authorized { accessToken => batchProcessingApi.startBatchProcess(batchRequestId, accessToken) }

    batchRequestId
  }

  def start_batch_process_cached(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                 from_datetime: String, until_datetime: String, band_names: util.List[String],
                                 sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                                 processing_options: util.Map[String, Any], subfolder: String,
                                 collecting_folder: String): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process_cached(collection_id, dataset_id, polygons, polygonsCrs, from_datetime, until_datetime,
      band_names, sampleType, metadata_properties, processing_options, subfolder, collecting_folder)
  }

  // TODO: move to the CachingService? What about the call to start_batch_process() then?
  def start_batch_process_cached(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                 crs: CRS, from_datetime: String, until_datetime: String, band_names: util.List[String],
                                 sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                                 processing_options: util.Map[String, Any], subfolder: String,
                                 collecting_folder: String): String = {
    require(metadata_properties.isEmpty, "metadata_properties are not supported yet")

    val from = ZonedDateTime.parse(from_datetime, ISO_OFFSET_DATE_TIME)
    val until = ZonedDateTime.parse(until_datetime, ISO_OFFSET_DATE_TIME)

    val cacheOperation =
      if (Set("sentinel-2-l2a", "S2L2A") contains dataset_id)
        new Sentinel2L2AInitialCacheOperation(dataset_id)
      else if (Set("sentinel-1-grd", "S1GRD") contains dataset_id) {
        // https://forum.sentinel-hub.com/t/sentinel-hub-december-2022-improvements/6198
        val defaultDemInstance = "COPERNICUS"
        new Sentinel1GrdInitialCacheOperation(dataset_id, defaultDemInstance)
      } else throw new IllegalArgumentException(
        """only datasets "sentinel-2-l2a" (previously "S2L2A") and
          | "sentinel-1-grd" (previously "S1GRD") are supported""".stripMargin)

    cacheOperation.startBatchProcess(collection_id, dataset_id, polygons, crs, from, until, band_names,
      sampleType, metadata_properties, processing_options, bucketName, subfolder, collecting_folder, this)
  }

  def get_batch_process(batch_request_id: String): BatchProcess = authorized { accessToken =>
    val response = new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken)
    BatchProcess(response.id, response.status, response.valueEstimate.map(_.bigDecimal).orNull,
      response.errorMessage.orNull)
  }

  //noinspection ScalaUnusedSymbol
  def restart_partially_failed_batch_process(batch_request_id: String): Unit = authorized { accessToken =>
    new BatchProcessingApi(endpoint).restartPartiallyFailedBatchProcess(batch_request_id, accessToken)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                   from_datetime: String, until_datetime: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, util.Map[String, Any]],
                                   subfolder: String, request_group_uuid: String): util.List[String] = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_card4l_batch_processes(collection_id, dataset_id, polygons, polygonsCrs, from_datetime, until_datetime,
      band_names, dem_instance, metadata_properties, subfolder, request_group_uuid)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                   crs: CRS, from_datetime: String, until_datetime: String,
                                   band_names: util.List[String], dem_instance: String,
                                   metadata_properties: util.Map[String, util.Map[String, Any]], subfolder: String,
                                   request_group_uuid: String): util.List[String] = {
    // TODO: add error handling
    val card4lId = UUID.fromString(request_group_uuid)

    val geometry = simplify(polygons).reproject(crs, LatLng)
    val geometryCrs = LatLng

    // from should be start of day, to should be end of day (23:59:59)
    val (from, to) = parseToInclusiveTemporalInterval(from_datetime, until_datetime)

    // original features that overlap in space and time
    val features = authorized { accessToken =>
      new DefaultCatalogApi(endpoint).searchCard4L(collection_id, geometry, geometryCrs, from, to,
        accessToken, Criteria.toQueryProperties(metadata_properties, collection_id))
    }

    // their intersections with input polygons (all should be in LatLng)
    val intersectionFeatures = features.mapValues(feature =>
      feature.mapGeom(geom => geom intersection geometry)
    )

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    // TODO: the web tool creates one batch process, analyses it, polls until ANALYSIS_DONE, then creates the remaining
    //  processes and starts them all
    val batchRequestIds =
      for ((id, Feature(intersection, featureData)) <- intersectionFeatures)
        yield authorized { accessToken =>
          batchProcessingApi.createCard4LBatchProcess(
            dataset_id,
            bounds = intersection,
            dateTime = featureData.dateTime,
            band_names.asScala,
            dataTakeId(id),
            card4lId,
            dem_instance,
            additionalDataFilters = Criteria.toDataFilters(metadata_properties),
            bucketName,
            subfolder,
            accessToken
          ).id
        }

    for (batchRequestId <- batchRequestIds) {
      authorized { accessToken => batchProcessingApi.startBatchProcess(batchRequestId, accessToken) }
    }

    batchRequestIds.toIndexedSeq.asJava
  }

  private def dataTakeId(featureId: String): String = {
    val penultimatePart = featureId.split("_").reverse(1) // from source at https://apps.sentinel-hub.com/s1-card4l/
    penultimatePart
  }
}
