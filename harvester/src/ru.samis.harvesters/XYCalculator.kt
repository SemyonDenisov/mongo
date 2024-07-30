package ru.samis.harvesters

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import org.bson.Document
import org.geotools.geojson.geom.GeometryJSON
import org.json.JSONObject
import org.locationtech.jts.io.WKTReader
import java.sql.DriverManager
import java.sql.Statement


class XYCalculator : Harvester() {
    private var postgreConnString = params.getString("postgreConnString")

    override fun mainHarvest(): Int {
        val houses = housesCollection.find(
            Filters.ne("Geometry", null)
        )
            .projection(Document("Geometry", 1))
//            .limit(10)
//            .toList()

//        val totalCount = houses.size
        val totalCount = houses.count()
        var counter = 0
        val reader = WKTReader()
        val geometryJSON = GeometryJSON()
        withPostgre { statement ->
            for (house in houses) {
                val geom = house["Geometry"] as Document
                val jsonGeometry = geometryJSON.read(geom.toJson())
                val wkt = jsonGeometry.toText()

//                statement.executeQuery("SELECT ST_AsText(ST_Centroid('$wkt'))").use { resultSet ->
                statement.executeQuery("SELECT ST_AsText(ST_PointOnSurface('$wkt'))").use { resultSet ->
                    resultSet.next()
                    val wktCentroid = resultSet.getString(1)
                    val centroidGeom = reader.read(wktCentroid)
                    val centroid = JSONObject(geometryJSON.toString(centroidGeom))
                    val coords = centroid.getJSONArray("coordinates")
//                    house["x"] = lon2merc(coords.getDouble(0))
//                    house["y"] = lat2merc(coords.getDouble(1))
                    housesCollection.updateOne(
                        Filters.eq("_id", house["_id"]),
                        Updates.combine(
                            Updates.set("x", lon2merc(coords.getDouble(0))),
                            Updates.set("y", lat2merc(coords.getDouble(1)))
                        )
                    )
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

        return totalCount
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