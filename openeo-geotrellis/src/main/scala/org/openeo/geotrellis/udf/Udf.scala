package org.openeo.geotrellis.udf

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.{ArrayTile, MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import jep.{DirectNDArray, SharedInterpreter}

import java.nio.ByteBuffer
import java.util

object Udf {

  private val defaultImports =
    """
      |import collections
      |import numpy as np
      |import xarray as xr
      |import openeo.metadata
      |from openeo.udf import UdfData
      |from openeo.udf.xarraydatacube import XarrayDataCube
      |""".stripMargin

  private def _createExtent(interp: SharedInterpreter, layoutDefinition: LayoutDefinition, key: SpatialKey): Unit = {
    interp.set("ex", layoutDefinition.extent)
    interp.set("tileLayout", layoutDefinition.tileLayout)
    interp.set("keyCol", key.col)
    interp.set("keyRow", key.row)

    val code =
      """
        |SpatialExtent = collections.namedtuple("SpatialExtent", ["top", "bottom", "right", "left", "height", "width"])
        |x_range = ex.xmax() - ex.xmin()
        |xinc = x_range / tileLayout.layoutCols()
        |yrange = ex.ymax() - ex.ymin()
        |yinc = yrange / tileLayout.layoutRows()
        |extent = SpatialExtent(
        |    top=ex.ymax() - yinc * keyRow,
        |    bottom=ex.ymax() - yinc * (keyRow + 1),
        |    right=ex.xmin() + xinc * (keyCol + 1),
        |    left=ex.xmin() + xinc * keyCol,
        |    height=tileLayout.tileCols(),
        |    width=tileLayout.tileRows()
        |)
        |""".stripMargin

    interp.exec(code)
  }

  private def _tileToDatacube(interp: SharedInterpreter, tile_shape: Array[Int], directTile: DirectNDArray[ByteBuffer],
                        band_names: util.ArrayList[String], start_times: Array[String] = Array()): Unit = {
    // Note: This method is a scala implementation of geopysparkdatacube._tile_to_datacube.
    interp.set("tile_shape", tile_shape)
    interp.set("start_times", start_times)
    interp.set("band_names", band_names)

    // Initialize coordinates and dimensions for the final xarray datacube.
    // TODO: Add message to OpenEOApiException:
    // \"\"\"In run_udf, the data has {b} bands, while the 'bands' dimension has {len_dim} labels. These labels were set on the dimension: {labels}. Please investigate if dimensions and labels are correct.\"\"\".format(b=band_count, len_dim = len(band_names), labels=str(band_names))
    interp.exec(
      """
        |coords = {}
        |dims = ('bands','y', 'x')
        |
        |# time coordinates if exists
        |if len(tile_shape) == 4:
        |    #we have a temporal dimension
        |    coords = {'t':start_times}
        |    dims = ('t' ,'bands','y', 'x')
        |
        |# band names if exists
        |if band_names:
        |    coords['bands'] = band_names
        |    band_count = tile_shape[dims.index('bands')]
        |    if band_count != len(band_names):
        |        raise OpenEOApiException(status_code=400,message='')
        |
        |if extent is not None:
        |    gridx=(extent.right-extent.left)/extent.width
        |    gridy=(extent.top-extent.bottom)/extent.height
        |    xdelta=gridx*0.5*(tile_shape[-1]-extent.width)
        |    ydelta=gridy*0.5*(tile_shape[-2]-extent.height)
        |    xmin=extent.left   -xdelta
        |    xmax=extent.right  +xdelta
        |    ymin=extent.bottom -ydelta
        |    ymax=extent.top    +ydelta
        |    coords['x']=np.linspace(xmin+0.5*gridx,xmax-0.5*gridx,tile_shape[-1],dtype=np.float32)
        |    coords['y']=np.linspace(ymax-0.5*gridy,ymin+0.5*gridy,tile_shape[-2],dtype=np.float32)
        |""".stripMargin)

    // Create a Datacube using the same area in memory as the Scala tile.
    interp.set("npCube", directTile)
    interp.exec(
      """
        |the_array = xr.DataArray(npCube, coords=coords, dims=dims, name="openEODataChunk")
        |datacube = XarrayDataCube(the_array)
        |""".stripMargin)
  }

  def runUserCode(code: String, layer: MultibandTileLayerRDD[SpatialKey],
                  bandNames: util.ArrayList[String], context: util.HashMap[String, Any]): MultibandTileLayerRDD[SpatialKey] = {

    // Map a python function to every tile of the RDD.
    // Map will serialize + send partitions to worker nodes
    // Worker nodes will receive partitions in JVM
    //  * We can use JEP to start a python interpreter in JVM
    //  * Then transfer the partition from the JVM to the interpreter using JEP

    // TODO: AllocateDirect is an expensive operation, we should create one buffer for the entire partition
    // and then slice it!
    val result = layer.mapPartitions(iter => {
      // TODO: Start an interpreter for every partition
      // TODO: Currently this fails because per tile processing cannot access the interpreter
      // TODO: This is because every tile in a partition is handled in a separate thread.
      iter.map(tuple => {
        val interp: SharedInterpreter = new SharedInterpreter
        val multiBandTile: MultibandTile = tuple._2
        var resultMultiBandTile = multiBandTile
        try {
          // Convert tile to DirectNDArray
          var bytes: Array[Byte] = Array()
          multiBandTile.bands.foreach((tile: Tile) => { bytes ++= tile.toBytes() })
          val buffer = ByteBuffer.allocateDirect(bytes.length) // Allocating a direct buffer is expensive.
          buffer.put(bytes)
          val tileShape = Array(multiBandTile.bandCount, multiBandTile.bands(0).cols, multiBandTile.bands(0).rows)
          val directTile = new DirectNDArray(buffer, tileShape: _*)

          // Setup the xarray datacube
          interp.exec(defaultImports)
          _createExtent(interp, layer.metadata.layout, tuple._1)
          _tileToDatacube(interp, tileShape, directTile, bandNames, Array())

          interp.set("context", context)
          interp.exec("data = UdfData(proj={\"EPSG\": 900913}, datacube_list=[datacube], user_context=context)")
          interp.exec(code)
          interp.exec("result_cube = apply_datacube(data.get_datacube_list()[0], data.user_context)")

          // Copy the result back to java heap memory.
          // In the future we can hopefully keep it in native memory from deserialization to serialization.
          val resultData: Array[Byte] = Array.fill(bytes.length)(0)
          buffer.rewind()
          for (i <- bytes.indices) {
            resultData(i) = buffer.get
          }

          // Convert the result back to a MultibandTile.
          resultMultiBandTile = multiBandTile.mapBands((bandNumber, tile) => {
            val tileSize = tile.dimensions.cols * tile.dimensions.rows
            val tileOffset = bandNumber * tileSize
            val tileData = resultData.slice(tileOffset, tileOffset + tileSize)
            ArrayTile(tileData, tile.dimensions.cols, tile.dimensions.rows)
          })
        } finally if (interp != null) interp.close()

        (tuple._1, resultMultiBandTile)
      })
    }, preservesPartitioning = true)

    ContextRDD(result, layer.metadata)
  }


}