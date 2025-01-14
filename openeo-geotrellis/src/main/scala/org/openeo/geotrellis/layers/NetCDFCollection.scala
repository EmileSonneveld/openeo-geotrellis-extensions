package org.openeo.geotrellis.layers

import com.azavea.gdal.GDALWarp
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TemporalProjectedExtent, TileBounds, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.raster.gdal.{DefaultDomain, GDALException, GDALRasterSource, MalformedProjectionException}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.{ByTileSpacetimePartitioner, ByTileSpatialPartitioner, DataCubeParameters}
import org.openeo.opensearch.OpenSearchClient

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.collection.immutable

object NetCDFCollection {

  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters,osClient:OpenSearchClient): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val sc = SparkContext.getOrCreate()
    val cube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = loadCollection(osClient, sc)
    Seq((0, cube))
  }

  def loadCollection(osClient:OpenSearchClient,sc: SparkContext) = {
    val stacItems = osClient.getProducts("", None, null, Map[String, Any](), "", "")
    val items = sc.parallelize(stacItems)

    sc.setJobDescription(s"load_stac from netCDFs - ${stacItems.head.id} - ${stacItems.head.links.head.href}")
    val resolutions = items.flatMap(_.resolution).distinct().collect()
    if(resolutions.length != 1) {
      throw new IllegalArgumentException("All items in a collection must have the same resolution")
    }

    val crs = items.flatMap(_.crs).distinct().collect()
    if(crs.length != 1) {
      throw new IllegalArgumentException("All items in a collection must have the same CRS")
    }

    val bboxWGS84: Extent = items.map(_.bbox).reduce((a, b)=>(a.combine(b)))


    val features: RDD[(TemporalProjectedExtent, MultibandTile)] = items.repartition(stacItems.length).flatMap(f=>{
      val allTiles = f.links.flatMap(l=>{
        l.bandNames.get.flatMap(b=> {
          var gdalNetCDFLink = s"${l.href.toString.replace("file:", "NETCDF:")}:${b}"
          if(!gdalNetCDFLink.startsWith("NETCDF:")) {
            gdalNetCDFLink = s"NETCDF:${gdalNetCDFLink}"
          }
          try{

            val rs = GDALRasterSource(gdalNetCDFLink)

            /**
             * Retrieving metadata using dataset directly, because sometimes metadata is so large that it doesn't fit the array allocated by GDALWarp
             */
            val units = rs.dataset.getMetadataItem("t#units",DefaultDomain,0)
            val conventions: String = rs.dataset.getMetadataItem("NC_GLOBAL#Conventions",DefaultDomain,0)
            val extraDim = rs.dataset.getMetadataItem("NETCDF_DIM_EXTRA",DefaultDomain,0)

            if (!conventions.startsWith("CF-1")) {
              throw new IllegalArgumentException(s"Only netCDF files with CF-1.x conventions are supported by this openEO backend, but found ${conventions}.")
            }
            if (extraDim != "{t}") {
              throw new IllegalArgumentException("Only netCDF files with a time dimension named 't' are supported by this openEO backend.")
            }
            if( units != "days since 1990-01-01") {
              throw new IllegalArgumentException("Only netCDF files with a time dimension in 'days since 1990-01-01' are supported by this openEO backend.")
            }
            val bandCount: Int = rs.dataset.bandCount

            //there's also a metadata item containing all timesteps, but it doesn't work on cluster for unknown reason
            val timeValues = (1 to bandCount).map(b=>{rs.dataset.getMetadataItem("NETCDF_DIM_t",DefaultDomain,b).toInt})

            val timestamps = timeValues.map(t=>{LocalDate.of(1990,1,1).atStartOfDay(ZoneId.of("UTC")).plusDays(t)})
            val raster: Raster[MultibandTile] = rs.read().get
            val temporalRasters: immutable.Seq[(ZonedDateTime, (String, ProjectedExtent, Tile))] = raster.tile.bands.zip(timestamps).map(rasterBand_time=>{
              (rasterBand_time._2,(b,ProjectedExtent(raster.extent,rs.crs),rasterBand_time._1))
            })
            temporalRasters
          }catch {
            case e: GDALException => {
              throw new IllegalArgumentException(s"load_stac/load_collection: GDAL gave an error for ${gdalNetCDFLink} with band $b. Error message: ${e.getMessage}", e)
            }
          }

        })})
        val byTime: Map[ZonedDateTime, Array[(String, ProjectedExtent, Tile)]] = allTiles.groupBy(_._1).mapValues(_.map(_._2))
        byTime.map(t=>{
          val sortedBands = t._2.sortBy(_._1)
          (TemporalProjectedExtent(t._2.head._2,t._1),MultibandTile(sortedBands.map(_._3)))
        })
      })

    val first = features.first()

    val cellType = first._2.cellType
    val extent = bboxWGS84.reproject(LatLng,crs(0))
    val layout = LayoutDefinition(RasterExtent(extent, CellSize(resolutions(0), resolutions(0))), 128)

    val spatialBounds = KeyBounds(layout.mapTransform(extent))
    val temporalBounds = KeyBounds(SpaceTimeKey(spatialBounds.minKey,TemporalKey(LocalDate.of(1990,1,1).atStartOfDay(ZoneId.of("UTC")))),SpaceTimeKey(spatialBounds.maxKey,TemporalKey(LocalDate.now().atStartOfDay(ZoneId.of("UTC")))))

    val partitioner: Partitioner = new SpacePartitioner(temporalBounds)(implicitly, implicitly, ByTileSpacetimePartitioner)

    val metadata = TileLayerMetadata[SpaceTimeKey](cellType, layout, extent, crs(0), temporalBounds)
    val retiled: RDD[(SpaceTimeKey, MultibandTile)] = features.tileToLayout(metadata).partitionBy(partitioner)
    ContextRDD(retiled,metadata)


  }
}
