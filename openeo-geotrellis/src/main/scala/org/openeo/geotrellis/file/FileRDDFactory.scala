package org.openeo.geotrellis.file

import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.OpenEOProcessScriptBuilder.AnyProcess
import org.openeo.geotrellis.{OpenEOProcessScriptBuilder, ProjectedPolygons}
import org.openeo.geotrelliscommon.DatacubeSupport.layerMetadata
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, DatacubeSupport, parseToInclusiveTemporalInterval}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.openeo.opensearch.backends.{CreodiasClient, OscarsClient}
import org.slf4j.LoggerFactory

import java.net.URL
import java.time.ZonedDateTime
import java.util
import scala.collection.JavaConverters._

/**
 * A class that looks like a pyramid factory, but does not build a full datacube. Instead, it generates an RDD[SpaceTimeKey, ProductPath].
 * This RDD can then be transformed into
 */
class FileRDDFactory(openSearch: OpenSearchClient, openSearchCollectionId: String,
                     attributeValues: util.Map[String, Any],
                     correlationId: String, val maxSpatialResolution: CellSize) {
  import FileRDDFactory._
  def this(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: util.List[String],
           attributeValues: util.Map[String, Any], correlationId: String) =
    this(openSearch, openSearchCollectionId, attributeValues, correlationId, maxSpatialResolution = CellSize(10, 10))

  /**
   * Lookup OpenSearch Features
   */
  private def getFeatures(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc: SparkContext): Seq[Feature] = {
    require(zoom >= 0)

    val overlappingFeatures: Seq[Feature] = openSearch.getProducts(
      openSearchCollectionId,
      Some((from, to)),
      boundingBox,
      attributeValues = attributeValues.asScala.toMap,
      correlationId, ""
    )
    overlappingFeatures
  }

  private def loadSpatialFeatureRDD(polygons: ProjectedPolygons, from_datetime: String, until_datetime: String, zoom: Int, tileSize: Int = 256, dataCubeParameters: DataCubeParameters): ContextRDD[SpaceTimeKey, Feature, TileLayerMetadata[SpaceTimeKey]] = {
    val sc = SparkContext.getOrCreate()

    val (from, to) = parseToInclusiveTemporalInterval(from_datetime, until_datetime)

    val bbox = polygons.polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons.crs)
    //load product metadata from OpenSearch
    var productMetadata: Seq[Feature] = getFeatures(boundingBox, from, to, zoom,sc)

    val filter = Option(dataCubeParameters).map(_.timeDimensionFilter)
    if (filter.isDefined && filter.get.isDefined) {
      val condition = filter.get.get.asInstanceOf[OpenEOProcessScriptBuilder].inputFunction.asInstanceOf[AnyProcess]
      //TODO how do we pass in user context
      val before = productMetadata.map(_.nominalDate).toSet
      productMetadata = productMetadata.filter(f => condition.apply(Map("value" -> f.nominalDate)).apply(f.nominalDate).asInstanceOf[Boolean])
      val after = productMetadata.map(_.nominalDate).toSet
      logger.info("Dates removed by timeDimensionFilter: " + (before -- after).mkString(","))
    }

    //construct layer metadata
    //hardcoded celltype of float: assuming we will generate floats in further processing
    //use a floating layout scheme, so we will process data in original utm projection and 10m resolution
    val multiple_polygons_flag = polygons.polygons.length > 1
    val metadata: TileLayerMetadata[SpaceTimeKey] = layerMetadata(
      boundingBox, from, to, 0, FloatConstantNoDataCellType, FloatingLayoutScheme(tileSize),
      maxSpatialResolution, Option(dataCubeParameters).flatMap(_.globalExtent) ,multiple_polygons_flag = multiple_polygons_flag
    )

    //construct Spatial Keys that we want to load
    val requiredKeys: RDD[SpatialKey] = sc.parallelize(polygons.polygons)
      .map(_.reproject(polygons.crs, metadata.crs))
      .clipToGrid(metadata.layout)
      .groupByKey()
      .keys

    val productsRDD = sc.parallelize(productMetadata)

    val spaceTimeRDD: RDD[(SpaceTimeKey, Feature)] = requiredKeys.cartesian(productsRDD)
      .map { case (SpatialKey(col, row), feature) => (SpaceTimeKey(col, row, feature.nominalDate), feature) }
      .filter { case (spaceTimeKey, feature) =>
        val keyExtent = metadata.mapTransform.keyToExtent(spaceTimeKey.spatialKey)
        ProjectedExtent(feature.bbox, LatLng).reproject(metadata.crs).intersects(keyExtent)
      }

    BatchJobMetadataTracker.tracker("").addInputProducts(openSearchCollectionId,
      spaceTimeRDD.values.map(_.id).distinct().collect().toList.asJava)

    val partitioner = DatacubeSupport.createPartitioner(None, spaceTimeRDD.keys, metadata)
    /**
     * Note that keys in spaceTimeRDD are not necessarily unique, because one date can have multiple Sentinel-1 products
     */
    new ContextRDD(spaceTimeRDD.partitionBy(partitioner.get), metadata)
  }

  /**
   * Variant of `loadSpatialFeatureRDD` that allows working with the data in JavaRDD format in PySpark context:
   * (e.g. oscars response is JSON-serialized)
   */
  def loadSpatialFeatureJsonRDD(polygons: ProjectedPolygons, from_datetime: String, until_datetime: String, zoom: Int, tileSize: Int = 256, dataCubeParameters: DataCubeParameters = null): (JavaRDD[String], TileLayerMetadata[SpaceTimeKey]) = {
    import org.openeo.geotrellis.file.FileRDDFactory.{jsonObject, toJson}
    val crdd = loadSpatialFeatureRDD(polygons, from_datetime, until_datetime, zoom, tileSize, dataCubeParameters)
    val jrdd = crdd.map { case (key, feature) => jsonObject(
      "key" -> toJson(key),
      "key_extent" -> toJson(crdd.metadata.mapTransform.keyToExtent(key)),
      "feature" -> jsonObject(
        "id" -> toJson(feature.id),
        "bbox" -> toJson(feature.bbox),
        "nominalDate" -> toJson(feature.nominalDate.toLocalDate.toString),
        "links" -> toJson(feature.links)
      ),
      "metadata" -> jsonObject(
        "extent" -> toJson(crdd.metadata.extent),
        "crs_epsg" -> crdd.metadata.crs.epsgCode.getOrElse(0).toString
      )
    )}.toJavaRDD()

    (jrdd, crdd.metadata)
  }
}

object FileRDDFactory {
  // Ignore trailing $'s in the class names for Scala objects
  private implicit val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def oscars(openSearchCollectionId: String, openSearchLinkTitles: util.List[String], attributeValues: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String = ""): FileRDDFactory = {
    val openSearch: OpenSearchClient = new OscarsClient(new URL("https://services.terrascope.be/catalogue"))
    new FileRDDFactory(openSearch, openSearchCollectionId, openSearchLinkTitles, attributeValues, correlationId = correlationId)
  }

  def creo(openSearchCollectionId: String, openSearchLinkTitles: util.List[String], attributeValues: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String = ""): FileRDDFactory = {
    val openSearch: OpenSearchClient = CreodiasClient()
    new FileRDDFactory(openSearch, openSearchCollectionId, openSearchLinkTitles, attributeValues, correlationId = correlationId)
  }

  /*
   * Poor man's JSON serialization
   * TODO: can we reuse some more standard/general JSON-serialization functionality?
   */

  def toJson(s: String): String =
    "\"" + s + "\""

  def toJson(s: Option[String]): String =
    s.map(toJson).getOrElse("null")

  def toJson(k: SpaceTimeKey): String =
    s"""{"col": ${k.col}, "row": ${k.row}, "instant": ${k.instant} }"""

  def toJson(e: Extent): String =
    s"""{"xmin": ${e.xmin}, "ymin": ${e.ymin}, "xmax": ${e.xmax}, "ymax": ${e.ymax}}"""

  def toJson(l: Link): String =
    s"""{"href": {"file": ${toJson(l.href.toString)}}, "title": ${toJson(l.title)}}"""

  def toJson(a: Array[Link]): String =
    "[" + a.map(toJson).mkString(",") + "]"

  /**
   * Helper to build JSON object (values should already be JSON-serialized)
   */
  def jsonObject(items: (String, String)*): String =
    "{" + items.map { case (k, v) => toJson(k) + ": " + v }.toList.mkString(",") + "}"
}