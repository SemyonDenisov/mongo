package ru.samis.harvesters.land

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.FragmentationHarvester
import java.io.File
import java.util.*


class IngeoHarvester : FragmentationHarvester() {
    private val regionName = params.getString("region")
//    private var cadNumKey = "CAD_NUM"

    override fun mainHarvest(): Int {
        housesCollection.drop()
        var inserted = parseGeoJSON()

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


    private fun parseGeoJSON(): Int {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(
            File(
                settings.getJSONObject("options").getString("catalog") +
                        params.getString("org/geotools/geojson")
            )
        )

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
        var number = ""
        var liter = ""
        var corp = ""
        var street = ""
        var number2 = ""
        var liter2 = ""
        var corp2 = ""
        var street2 = ""
        var municipality = ""
        var extraProps = Document()

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
                                extraProps = Document()
                                doc = Document()
                                    .append("Region", regionName)
                                    .append("RegionID", regionID)
                                    .append("Municipalitet", null)
                                    .append("MunicipalitetID", null)
                                    .append("BlockID", null)
//                                    .append("Solid", 1)
                                    .append("Street", null)
                                    .append("StreetID", null)
                                    .append("HouseNumber", null)
                                    .append("PostalCode", null)
                                    .append("ExtraProps", extraProps)
                                    .append("RegionCode", regionCode)
                                counter = 0
                                number = ""
                                liter = ""
                                corp = ""
                                street = ""
                                number2 = ""
                                liter2 = ""
                                corp2 = ""
                                street2 = ""
                                municipality = ""
                            }
                        }
                    }

                    "ID" -> {
                        proc = { doc.append("ID", parser.valueAsString) }
                    }

                    "NP" -> {
                        proc = {
                            municipality = parser.valueAsString
                            doc.append("Municipalitet", municipality)
                                .append("MunicipalitetID", DigestUtils.sha1Hex(municipality))
                        }
                    }

                    "ULICA_1" -> {
                        proc = {
                            street = parser.valueAsString
                            doc.append("Street", street)
                        }
                    }

                    "NOMER_1" -> {
                        proc = { number = parser.valueAsString }
                    }

                    "LITER_1" -> {
                        proc = { liter = parser.valueAsString }
                    }

                    "KORPUS_1" -> {
                        proc = { corp = parser.valueAsString }
                    }

                    "SOLID" -> {
                        proc = { doc.append("Solid", parser.valueAsString.toIntOrNull()) }
                    }

                    "ULICA_2" -> {
                        proc = {
                            street2 = parser.valueAsString
//                            doc.append("Street", street)
                        }
                    }

                    "NOMER_2" -> {
                        proc = { number2 = parser.valueAsString }
                    }

                    "LITER_2" -> {
                        proc = { liter2 = parser.valueAsString }
                    }

                    "KORPUS_2" -> {
                        proc = { corp2 = parser.valueAsString }
                    }

                    "ETAJNOST", "PEOPLE_COUNT", "FLATS_COUNT" -> {
                        proc = {
                            extraProps.append(
                                parser.currentName,
                                parser.valueAsString.toIntOrNull() ?: parser.valueAsString
                            )
                        }
                    }

                    "MATERIAL" -> {
                        proc = { extraProps.append(parser.currentName, parser.valueAsString) }
                    }

                    "SQUARE" -> {
                        proc = { extraProps.append(parser.currentName, parser.valueAsDouble) }
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
                    if (inFeature && street.isNotBlank()) {
                        var houseNum = number
                        if (liter.isNotBlank())
                            houseNum += liter
                        if (corp.isNotBlank())
                            houseNum += " корп $corp"

                        doc.append("HouseNumber", houseNum)
                            .append("StreetID", DigestUtils.sha1Hex(municipality + street))

                        if (street2.isNotBlank()) {
                            var houseNum2 = number2
                            if (liter2.isNotBlank())
                                houseNum2 += liter2
                            if (corp2.isNotBlank())
                                houseNum2 += " корп $corp2"

                            buffer += Document(doc)
                                .append("HouseNumber", houseNum2)
                                .append("Street", street2)
                                .append("StreetID", DigestUtils.sha1Hex(municipality + street2))
                            inserted++
                        }

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