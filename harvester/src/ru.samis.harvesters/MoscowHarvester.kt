package ru.samis.harvesters

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import java.io.File
import java.io.InputStreamReader


class MoscowHarvester : Harvester() {
    protected val regionName = "город Москва"
    protected val regionID = DigestUtils.sha1Hex(regionName)

    override fun mainHarvest(): Int {
        housesCollection.drop()

        val inserted = parseJSON()

        housesCollection.apply {
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("MunicipalitetID"))
            createIndex(Indexes.ascending("StreetID"))
            createIndex(Indexes.ascending("BlockID"))
            createIndex(Indexes.ascending("cadNum"))
            createIndex(Indexes.ascending("Region"))
            createIndex(Indexes.ascending("Municipalitet"))
            createIndex(Indexes.ascending("Street"))
            createIndex(Indexes.ascending("HouseNumber"))

            createIndex(
                Indexes.compoundIndex(
                    Indexes.text("Region"),
                    Indexes.text("Municipalitet"),
                    Indexes.text("Street"),
                    Indexes.text("HouseNumber")
                )
            )
        }


        return inserted
    }

    private fun parseJSON(): Int {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(
            InputStreamReader(File(settings.getJSONObject("options").getString("file")).inputStream(), "Windows-1251")
        )

        var doc = Document()
        var counter = 0
        var geomCounter = 0
        var geomPointsCounter = 0
        val geomHierarchy = mutableListOf<MutableList<Any>>()
        var geometryStarted = false
        var inFeature = false
        var proc: ((JsonToken) -> Unit)? = null
        var lat = 0.0
        var lon = 0.0
        var inserted = 0
        val props = mutableMapOf<String, Any?>()

        while (!parser.isClosed) {
            var token = parser.nextToken()

            when (token) {
                JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_INT -> {
                    proc?.invoke(token)
                    proc = null
                }

                else -> {}
            }

            if (token == JsonToken.FIELD_NAME) { // в случае если токен строковое знание - выводим на экран
                when (val name = parser.currentName) {
                    "geoData" -> {
                        geometryStarted = true
                        geomCounter = 0
                        geomPointsCounter = 0
                        geomHierarchy.clear()
                        lat = 0.0
                        lon = 0.0
                    }

                    else -> {
                        proc = { token ->
                            props[name] = try {
                                when (token) {
                                    JsonToken.VALUE_STRING -> parser.valueAsString
                                    JsonToken.VALUE_NUMBER_INT -> parser.valueAsInt
                                    else -> null
                                }
                            } catch (e: Exception) {
                                null
                            }
                        }
                    }
                }
            }

            if (token == JsonToken.START_OBJECT) {
                if (counter == 0) {
                    geometryStarted = false
                    inFeature = true
                    props.clear()
                    doc = Document()
                        .append("Region", regionName)
                        .append("RegionID", regionID)
                        .append("RegionCode", "77")
                }
                counter++
            }

            if (token == JsonToken.END_OBJECT) {
                counter--
                if (counter == 0) {
                    if (inFeature) {
                        var house = concat(
                            props,
                            arrayOf(
                                "L1_TYPE",
                                "L1_VALUE",
                                "L2_TYPE",
                                "L2_VALUE",
                                "L3_TYPE",
                                "L3_VALUE",
                                "L4_TYPE",
                                "L4_VALUE",
                                "L5_TYPE",
                                "L5_VALUE"
                            )
                        )
                        var mun = concat(props, arrayOf("P3", "P4", "P6"))
                        mun = if (mun.isNotBlank()) "$regionName $mun" else regionName
                        val street = props["P7"] as String?
                        doc.append("Municipalitet", mun)
                            .append("MunicipalitetID", DigestUtils.sha1Hex(mun))
                            .append("Street", street)
                            .append("StreetID", DigestUtils.sha1Hex(street ?: ""))
                            .append("BlockID", props["ADM_AREA"])
                            .append("HouseNumber", house)
                            .append("PostalCode", null)
                            .append("cadNum", props["KAD_N"])
                            .append("AddressDesc", props["ADDRESS"])
                            .append("ID", props["global_id"].toString())
                            .append("Cladr", props["KLADR"])
                            .append("Fias", props["N_FIAS"])
                            .append("Date", props["DREG"])

                        housesCollection.insertOne(doc)

                        inserted++
                        if (inserted % 10000 == 0) {
                            println("$inserted inserted")
                            var progress = inserted / 481609.0
                            if (progress > 0.99) progress = 0.99
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
//                        doc.append("trueGeometry", trueGeometry(geomHierarchy[0]))
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
        fun concat(parts: MutableMap<String, Any?>, keys: Array<String>): String {
            var result = ""

            for (key in keys) {
                val part = parts[key] ?: continue
                if (part !is String || part.isNotEmpty()) result += " $part"
            }
            return if (result.isNotEmpty()) result.substring(1) else ""
        }
    }
}