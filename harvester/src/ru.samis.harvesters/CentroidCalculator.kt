package ru.samis.harvesters



import com.mongodb.client.model.Filters
import org.bson.Document
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.jts.io.WKTReader
import java.io.StringWriter
import java.sql.DriverManager
import java.sql.Statement


class CentroidCalculator : ru.samis.harvesters.Harvester() {
    private var postgreConnString = params.getString("postgreConnString")

    override fun mainHarvest(): Int {
        val houses = housesCollection.find(
            Filters.ne("geometry", null)
        ).toList()

        val totalCount = houses.size
        var counter = 0
        val reader = WKTReader()
        val geometryJSON = GeometryJSON()
        withPostgre { statement ->
            for (house in houses) {
                val geom = house["geometry"] as Document
                val jsonGeometry = geometryJSON.read(geom.toJson())
                val wkt = jsonGeometry.toText()

//                statement.executeQuery("SELECT ST_AsText(ST_Centroid('$wkt'))").use { resultSet ->
                statement.executeQuery("SELECT ST_AsText(ST_PointOnSurface('$wkt'))").use { resultSet ->
                    resultSet.next()
                    val wktCentroid = resultSet.getString(1)
                    val centroidGeom = reader.read(wktCentroid)
                    val stringWriter = StringWriter()
                    geometryJSON.write(centroidGeom, stringWriter)
                    house["centroid"] = Document.parse(stringWriter.toString())
                    housesCollection.replaceOne(Filters.eq("_id", house["_id"]), house)
                }
//            sumCoords(geom["coordinates"] as List<*>)
//            lng /= count
//            lat /= count
//
//            val centroid = Document("type", "Point")
//                .append("coordinates", listOf(lng, lat))
//            house["centroid"] = centroid
//            housesCollection.replaceOne(Filters.eq("_id", house["_id"]), house)

                counter++
                if (counter % 1000 == 0) {
                    writeProgress(counter * 1.0 / totalCount, totalCount)
                    println(counter)
                }
            }
        }

        return houses.size
    }

    private var lng = 0.0
    private var lat = 0.0
    private var count = 0

    private fun sumCoords(coords: List<*>) {
        if (coords.isEmpty()) return

        if (coords[0] is List<*>) {
            for (list in coords) {
                sumCoords(list as List<*>)
            }
        } else {
//            if (coords[0] is Double) {
            lng += coords[0].toString().toDouble()
            lat += coords[1].toString().toDouble()
            count++
//            }
        }


    }


    fun <T> withPostgre(callback: (Statement) -> T): T {
        Class.forName("org.postgresql.Driver")

        return DriverManager.getConnection(postgreConnString).use {
            it.createStatement().use(callback)
        }
    }

    companion object {
        fun concat(parts: List<String>, start: Int = 0, end: Int = parts.lastIndex): String {
            var result = ""

            for (i in start..end) {
                val part = parts[i]
                if (part.isNotEmpty()) result += " $part"
            }
            return if (result.isNotEmpty()) result.substring(1) else ""
        }
    }
}