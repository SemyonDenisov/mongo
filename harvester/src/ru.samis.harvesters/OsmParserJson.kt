package ru.samis.harvesters

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import java.io.File


class OsmParserJson : FragmentationHarvester() {

    override fun mainHarvest(): Int {

        try {
            insertMetadata(datasetStructure)

            housesCollection.drop()
            val stub = Document()
            housesCollection.insertOne(stub)
//            buffer.clear()
            createIndexes()


            val jsonFactory = JsonFactory()
            val parser = jsonFactory.createParser(
                File(
                    settings.getJSONObject("options").getString("OsmCatalog") +
                            settings.getJSONObject("params").getString("regionFile")
                )
            )

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

                if (token == JsonToken.FIELD_NAME) { // в случае если токен строковое знание - выводим на экран
                    when (parser.currentName) {
                        "type" -> {
                            proc = {
                                if (parser.valueAsString == "Feature") {
                                    geometryStarted = false
                                    inFeature = true
                                    doc = Document()
                                        .append("Region", region)
//                                        .append("ID", housesCollection.countDocuments().toString())
                                        .append("RegionID", regionID)
                                        .append("Solid", 1)
                                        .append("RegionCode", regionCode)
                                    counter = 0
                                }
                            }
                        }

                        "ADDR_CITY" -> {
                            proc = {
                                doc.append("Municipalitet", parser.valueAsString ?: "")
                                    .append(
                                        "MunicipalitetID",
                                        DigestUtils.sha1Hex(region + (parser.valueAsString ?: ""))
                                    )
                            }
                        }

                        "A_STRT" -> {
                            proc = {
                                doc.append("Street", parser.valueAsString ?: "")
                            }
                        }

                        "A_HSNMBR" -> {
                            proc = { doc.append("HouseNumber", parser.valueAsString ?: "") }
                        }

                        "BUILDING" -> {
                            proc = { doc.append("BuildingType", parser.valueAsString ?: "") }
                        }

                        "B_LEVELS" -> {
                            proc = { doc.append("floorsCount", parser.valueAsString ?: "") }
                        }

                        "A_PSTCD" -> {
                            proc = { doc.append("PostalCode", parser.valueAsString ?: "") }
                        }

                        "OSM_ID" -> {
                            proc = { doc.append("ID", parser.valueAsString) }
                        }

                        "geometry" -> {
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
                        if (inFeature) {
                            try {
//                            buffer += doc
                                doc.append(
                                    "StreetID",
                                    DigestUtils.sha1Hex(region + doc["Municipalitet"] + doc["Street"])
                                )
                                housesCollection.insertOne(doc)
//                            if (buffer.size == 10) {
//                                housesCollection.insertMany(buffer)
//                                buffer.clear()
//                            }
                                inserted++
                                if (inserted % 10000 == 0) {
                                    println("$inserted inserted")
                                    var progress = inserted / 500000.0
                                    if (progress > 0.99) progress = 0.99
                                    writeProgress(progress, inserted)
                                }
                            } catch (e: Exception) {
                                System.err.println("id ${doc["ID"]} insertion error")
                                writeError(e.message ?: "")
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

            housesCollection.deleteOne(stub)

            calcSettlements()

            return inserted
        } catch (e: Exception) {
            writeError(e.message ?: "")
        }

        return 0
    }

    private fun createIndexes() {

        do {
            val error = try {
                with(housesCollection) {
                    createIndex(Indexes.ascending("ID"))
                    createIndex(Indexes.ascending("RegionID"))
                    createIndex(Indexes.ascending("MunicipalitetID"))
                    createIndex(Indexes.ascending("Municipalitet"))
                    createIndex(Indexes.ascending("StreetID"))
                    createIndex(Indexes.ascending("BlockID"))
                    createIndex(Indexes.geo2dsphere("Geometry"))
                    createIndex(
                        Indexes.compoundIndex(
                            Indexes.text("Region"),
                            Indexes.text("Municipalitet"),
                            Indexes.text("Street"),
                            Indexes.text("HouseNumber")
                        )
                    )
                }
                false
            } catch (e: Exception) {
                housesCollection.drop()
                true
            }
        } while (error)
    }

    private fun calcSettlements() {
        for (settlement in regionSettlements) {
            val houses = housesCollection.find(
                Filters.and(
                    eq("Municipalitet", ""),
                    Filters.geoIntersects("Geometry", settlement["geometry"] as Document)
                )
            )
            val name = settlement["name"]
            println("$name ${houses.count()}")

            for (house in houses) {
                housesCollection.updateOne(
                    eq("ID", house["ID"]),
                    Updates.combine(
                        Updates.set("Municipalitet", name),
                        Updates.set(
                            "MunicipalitetID", DigestUtils.sha1Hex(region + name)
                        )
                    )
                )
            }
        }
    }

//    private val buffer = mutableListOf<Document>()

    companion object {
        val ERROR_MARKER = "Can't extract geo keys: "

        val datasetStructure = arrayOf(
            arrayOf("ID", "ID", "string"),
            arrayOf("Region", "Регион", "string"),
            arrayOf("RegionID", "ID региона", "string"),
            arrayOf("Municipalitet", "Населённый пункт", "string"),
            arrayOf("MunicipalitetID", "ID населённого пункта", "string"),
            arrayOf("Street", "Улица", "string"),
            arrayOf("StreetID", "ID улицы", "string"),
            arrayOf("PostalCode", "Почтовый индекс", "string"),
            arrayOf("BlockID", "Район", "string"),
            arrayOf("HouseNumber", "Номер дома", "string"),
            arrayOf("x", "x", "float"),
            arrayOf("y", "y", "float"),
            arrayOf("Geometry", "Геометрия", "geometry")
        )
    }

}