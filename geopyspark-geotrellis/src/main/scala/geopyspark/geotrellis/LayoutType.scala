package geopyspark.geotrellis

import geotrellis.layer.{FloatingLayoutScheme, LayoutDefinition, LayoutLevel, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.vector.Extent

/** Strategy for selecting LayoutScheme before metadata is collected */
sealed trait LayoutType {
  /** Produce the [[LayoutDefinition]] and zoom level, if applicable, for given raster */
  def layoutDefinitionWithZoom(crs: CRS, extent: Extent, cellSize: CellSize): (LayoutDefinition, Option[Int])

  /** Produce the [[LayoutDefinition]] for given raster */
  def layoutDefinition(crs: CRS, extent: Extent, cellSize: CellSize): LayoutDefinition =
    layoutDefinitionWithZoom(crs, extent, cellSize)._1
}

/** @see [[geotrellis.layer.ZoomedLayoutScheme]] */
case class GlobalLayout(tileSize: Int, zoom: Integer, threshold: Double)  extends LayoutType {
  def layoutDefinitionWithZoom(crs: CRS, extent: Extent, cellSize: CellSize) = {
    val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
    Option(zoom) match {
      case Some(zoom) =>
        scheme.levelForZoom(zoom).layout -> Some(zoom)
      case None =>
        val LayoutLevel(zoom, ld) = scheme.levelFor(extent, cellSize)
        ld -> Some(zoom)
    }
  }
}

/** @see [[geotrellis.layer.FloatingLayoutScheme]] */
case class LocalLayout(tileCols: Int, tileRows: Int) extends LayoutType {
  def layoutDefinitionWithZoom(crs: CRS, extent: Extent, cellSize: CellSize) = {
    val scheme = new FloatingLayoutScheme(tileCols, tileRows)
    scheme.levelFor(extent, cellSize).layout -> None
  }
}


object LocalLayout {
  def apply(tileSize: Int): LocalLayout =
    LocalLayout(tileSize, tileSize)
}
