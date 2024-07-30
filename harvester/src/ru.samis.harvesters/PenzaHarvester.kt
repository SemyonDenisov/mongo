package ru.samis.harvesters.land

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.FragmentationHarvester
import java.io.File
import java.util.*


class PenzaHarvester : FragmentationHarvester() {
    private val regionName = params.getString("region")
    private val municipalityName = "Пенза"
    private val municipalityID = DigestUtils.sha1Hex("Пенза")
//    private var cadNumKey = "CAD_NUM"

    override fun mainHarvest(): Int {
        housesCollection.drop()
        var inserted = parseGeoJSON(
            settings.getJSONObject("options").getString("catalog") +
                    params.getString(""),
            true
        )

        inserted += parseGeoJSON(
            settings.getJSONObject("options").getString("catalog") +
                    params.getString("geojson2"),
            false
        )



        housesCollection.apply {
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("MunicipalitetID"))
            createIndex(Indexes.ascending("StreetID"))
            createIndex(Indexes.ascending("cadNumInt"))
            createIndex(Indexes.ascending("cadNum"))
        }

        return inserted
    }


    private fun parseGeoJSON(path: String, checkSemantics: Boolean): Int {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(File(path))

        val buffer = mutableListOf<Document>()
        var doc = Document()
        var counter = 0
        var geomCounter = 0
        var geomPointsCounter = 0
        val geomHierarchy = mutableListOf<MutableList<Any>>()
        var geometryStarted = false
        var inFeature = false
        var proc: (() -> Unit)? = null
        var lat = 0.0
        var lon = 0.0
        var inserted = 0

        while (!parser.isClosed) {
            val token = parser.nextToken()

            when (token) {
                JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NULL, JsonToken.VALUE_NUMBER_INT -> {
                    proc?.invoke()
                    proc = null
                }

                else -> {}
            }

            if (token == JsonToken.FIELD_NAME) {
                when (parser.currentName.uppercase(Locale.getDefault())) {
                    "TYPE" -> {
                        proc = {
                            if (parser.valueAsString == "Feature") {
                                geometryStarted = false
                                inFeature = true
                                doc = Document()
                                    .append("Region", regionName)
                                    .append("RegionID", regionID)
                                    .append("Municipalitet", municipalityName)
                                    .append("MunicipalitetID", municipalityID)
                                    .append("BlockID", null)
//                                    .append("Solid", 1)
                                    .append("Street", null)
                                    .append("StreetID", null)
                                    .append("HouseNumber", null)
                                    .append("PostalCode", null)
                                    .append("RegionCode", regionCode)
                                counter = 0
                            }
                        }
                    }

                    "ObjectID" -> {
                        proc = { doc.append("ID", parser.valueAsString) }
                    }

                    "SEM31201" -> {
                        proc = {
                            val street = parser.valueAsString
                            doc.append("Street", street)
                                .append("StreetID", DigestUtils.sha1Hex(municipalityName + street))
                        }
                    }

                    "SEM31202" -> {
                        proc = { doc.append("HouseNumber", parser.valueAsString) }
                    }

                    "SEM247" -> {
                        proc = { doc.append("BuildingType", parser.valueAsString) }
                    }


                    "GEOMETRY" -> {
                        geometryStarted = true
                        geomCounter = 0
                        geomPointsCounter = 0
                        geomHierarchy.clear()
                        lat = 0.0
                        lon = 0.0
                    }
                }
            }

            if (token == JsonToken.START_OBJECT) counter++
            if (token == JsonToken.END_OBJECT) {
                counter--
                if (counter == -1) {
                    if (inFeature && (!checkSemantics || doc["Street"] != null && doc["HouseNumber"] != null)) {

                        buffer += doc
                        if (buffer.size >= 10) {
                            housesCollection.insertMany(buffer)
                            buffer.clear()
                        }
                        inserted++
                        if (inserted % 10000 <= 1) {
                            println("$inserted inserted")
                            var progress = inserted / 1000000.0 / 2
                            if (progress > 0.5) progress = 0.5
                            writeProgress(progress, inserted)
                        }
                    }
                    inFeature = false
                }
//                readLine()
            }

            if (token == JsonToken.START_ARRAY) {
                if (geometryStarted) {
                    val newList = mutableListOf<Any>()
                    geomHierarchy.lastOrNull()?.apply { add(newList) }
                    geomHierarchy += newList
                    geomCounter++
                }
            }

            if (token == JsonToken.END_ARRAY) {
                if (geometryStarted) {
                    geomCounter--
                    if (geomCounter > 0) {
                        val list = geomHierarchy.last()
                        if (list[0] is List<*> && (list[0] as List<*>)[0] is Double) {
                            for (i in 0 until list.size - 1) {
                                val point = list[i] as List<Double>
                                lat += point[1]
                                lon += point[0]
                                geomPointsCounter++
                            }

                        }
                        geomHierarchy.removeAt(geomHierarchy.lastIndex)
                    }
                    if (geomCounter == 0) {
                        geometryStarted = false


                        lat /= geomPointsCounter
                        lon /= geomPointsCounter

//                            if (!isInRegion(lat, lon)) continue

                        val type = try {
                            (((geomHierarchy[0][0] as List<*>)[0] as List<*>)[0] as List<*>)[0]
                            "MultiPolygon"
                        } catch (e: Exception) {
                            "Polygon"
                        }

                        val y = lat2merc(lat)
                        val x = lon2merc(lon)
                        doc.append(
                            "Geometry",
                            Document("type", type).append("coordinates", geomHierarchy[0])
                        )
                            .append("y", y)
                            .append("x", x)
                            .append("radius", avgRaduis(geomHierarchy[0], x, y))
                            .append("BlockID", findArea(lat, lon))
                    }
                }
            }

            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if (geomCounter > 0) {
                    geomHierarchy.last().add(parser.valueAsDouble)
                }
            }
        }

        return inserted
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