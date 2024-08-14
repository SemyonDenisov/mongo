package ru.samis.harvesters.oks

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.model.*
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.json.JSONArray
import ru.samis.harvesters.Harvester
import java.io.File
import kotlin.system.measureTimeMillis
import kotlin.math.min
import ru.samis.addressparser.AddressParser
abstract class OksHarvester : Harvester() {

    protected val regionName: String = params.getString("region")
    protected val regionID: String = DigestUtils.sha1Hex(regionName)
    protected var cadNumKey = "cad_num"
    protected var progressMult = 0.5

    //
    protected var geojsonWithSemantics: Boolean = params.optBoolean("geojsonWithSemantics", false)
    protected var geojsonSemanticFields: JSONArray = JSONArray()
    protected val srcNames = mutableListOf<String>()
    protected val dstNames = mutableListOf<String>()
    protected val fieldTypes = mutableListOf<String>()


    //
    protected val updatesMap = mutableMapOf<Long, List<String>>()

    override fun mainHarvest(): Int {
        cadNumKey = params.optString("cadNumKey", cadNumKey).toLowerCase()
        if (cadNumKey == "cad_num") {
            println("cadNum field name is set to default value: cad_num")
        }

        val parseAddress = params.optBoolean("parseAddress", false)
        println("parseAddress $parseAddress")
        progressMult = if (parseAddress) 0.25 else 0.5

        // {dataset}_inProgress
        if (updateCollections) {
            housesCollection.apply {
                drop()
                createIndex(Indexes.ascending("ID"))
                createIndex(Indexes.ascending("RegionID"))
                createIndex(Indexes.ascending("MunicipalitetID"))
                createIndex(Indexes.ascending("StreetID"))
                createIndex(Indexes.ascending("BlockID"))
                createIndex(Indexes.ascending("cadNumInt"))
                // createIndex(Indexes.ascending("cadNum"))
                createIndex(Indexes.ascending("Region"))
                createIndex(Indexes.ascending("Municipalitet"))
                createIndex(Indexes.ascending("Street"))
                createIndex(Indexes.ascending("HouseNumber"))
               // createIndex(Indexes.geo2dsphere("Geometry"))

                createIndex(
                    Indexes.compoundIndex(
                        Indexes.text("Region"),
                        Indexes.text("Municipalitet"),
                        Indexes.text("Street"),
                        Indexes.text("HouseNumber")
                    )
                )
            }
        }

        if (geojsonWithSemantics) {
            geojsonSemanticFields = params.getJSONArray("geojsonSemanticFields")
            for (i in 0 until geojsonSemanticFields.length()) {
                val field = geojsonSemanticFields.getJSONArray(i)
                srcNames.add(field.getString(0).toLowerCase())
                dstNames.add(field.getString(1))
                fieldTypes.add(field.optString(2, "string").toLowerCase())
            }
        }
        var inserted = 0
        var elapsedTime = measureTimeMillis {
            println("parsing Geojson")
            inserted += parseGeoJSON()
        }.toDouble()
        println("Elapsed time for geometry: ${elapsedTime / 1000} seconds")

        elapsedTime = measureTimeMillis {
            inserted += parseSemantics()
        }.toDouble()
        println("Elapsed time for semantics: ${elapsedTime / 1000} seconds")

        val keyField = params.optString("keyField", "cadNumInt")
        val filtersJSON = params.optString("filter")

        if (parseAddress) {
            AddressParser(
                params.getString("database"),
                params.getString("dataset") + "_inProgress",
                mutableListOf(params.optString("addressField", "AddressDesc")),
                mutableListOf(),
                keyField,
                if (filtersJSON.isNotEmpty()) Document.parse(filtersJSON) else Document()
            ).parseAddresses { count, totalCount ->
                val progress = 1.0 * count / totalCount
                writeProgress(0.5 + min(progress * 0.5, 0.49), inserted)
            }
        }
        return inserted
    }

    abstract fun parseSemantics(): Int

    private fun parseGeoJSON(): Int {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(
            File(settings.getJSONObject("options").getString("catalog") + params.getString("geojson"))
        )

        var insertDoc = Document()
        var semanticsDoc = Document()

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
        var updated = 0

        // fields to insert:
        /*
        ID, cadNumInt, BlockID, Region, RegionID, RegionCode, Geometry - обязательные поля, точно должны быть у любого ЗУ или ОКСа
        Geometry, x, y, radius - поля для insert'a

        т.о.
        перечисленные поля добавляю в insertDoc
        остальные в semanticsDoc и сохраняю в мапу для дальнейших апдейтов
         */

        while (!parser.isClosed) {
            var token = parser.nextToken()

            when (token) {
                JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NULL, JsonToken.VALUE_NUMBER_INT -> {
                    proc?.invoke()
                    proc = null
                }
                else -> {}
            }

            if (token == JsonToken.FIELD_NAME) { // в случае если токен строковое знание - выводим на экран
                when (parser.currentName.toLowerCase()) {
                    "type" -> {
                        proc = {
                            if (parser.valueAsString == "Feature") {
                                geometryStarted = false
                                inFeature = true
                                counter = 0

                                insertDoc = Document()
                                semanticsDoc = Document()
                            }
                        }
                    }

                    cadNumKey, "cad_num" -> {
                        proc = {
                            parser.valueAsString?.replace(".", ":")?.let { cadNum ->
                                if (cadNum.isBlank() || cadNum.toLowerCase() == "null") return@let
                                val cadNumInt = ((cadNum)
                                    .replace(":", "")
                                    .replace("(", "")
                                    .replace(")", "")
                                        ).toLongOrNull() ?: 0
                                val cadDigits = cadNum.split(":")

                                insertDoc.append("cadNumInt", cadNumInt)
                                    .append("ID", cadNum)
                                    .append(
                                        "BlockID",
                                        if (cadDigits.size >= 2) cadDigits.subList(0, 2)
                                            .joinToString(":") else cadNum
                                    )
                                    .append("Region", regionName)
                                    .append("RegionID", regionID)
                                    .append("RegionCode", regionCode)
                            }
                        }
                    }
                    "geometry" -> {
                        geometryStarted = true
                        geomCounter = 0
                        geomPointsCounter = 0
                        geomHierarchy.clear()
                        lat = 0.0
                        lon = 0.0
                    }

                    //TODO
                    else -> {
                        if (geojsonWithSemantics) {
                            if (srcNames.contains(parser.currentName.toLowerCase())) {
                                val dstName = dstNames[srcNames.indexOf(parser.currentName.toLowerCase())]
                                proc = {
                                    if (parser.valueAsString != null && parser.valueAsString.isNotBlank()) {
                                        val value =
                                            when (fieldTypes[srcNames.indexOf(parser.currentName.toLowerCase())]) {
                                                "int" -> try {
                                                    parser.valueAsString.replace("\\s".toRegex(), "")
                                                        .replace(" ;", "")
                                                        .trim().toInt()
                                                } catch (e: Exception) {
                                                    -1
                                                }
                                                "double" -> try {
                                                    parser.valueAsString.replace("\\s".toRegex(), "")
                                                        .replace(" ", "")
                                                        .trim().replace(",", ".").toDouble()
                                                } catch (e: Exception) {
                                                    -1
                                                }
                                                else -> parser.valueAsString.trim()
                                            }
                                        if (dstName != "Cost") {
                                            semanticsDoc.append(dstName, value)
                                        } else {
                                            if (value.toString().toDouble() > 1) semanticsDoc.append(dstName, value)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (token == JsonToken.START_OBJECT) counter++
            if (token == JsonToken.END_OBJECT) {
                counter--
                if (counter == -1) {
                    if (inFeature) {

                        if (insertDoc["cadNumInt"] != null) {

                            updatesMap[insertDoc["cadNumInt"] as Long] = semanticsDoc.keys.toList()

                            val combinedDoc = Document().apply {
                                putAll(insertDoc)
                                putAll(semanticsDoc)
                            }
                            if (updateCollections) housesCollection.insertOne(combinedDoc)
                            inserted++
                            incCount()
                            updated++
                            if (updated % 1000 == 0) writeUpdateProgress(updated, inserted)
                        }
                    }
                    inFeature = false
                }
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
                        if (list[0] is Double) {
                            lat += list[1] as Double
                            lon += list[0] as Double
                            geomPointsCounter++
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

                        insertDoc.append("Geometry", Document("type", type).append("coordinates", geomHierarchy[0]))
                            .append("y", y)
                            .append("x", x)
                            .append("radius", avgRaduis(geomHierarchy[0], x, y))
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
    fun writeUpdateProgress(updated: Int, inserted: Int) {
        println("$updated updated")
        var progress = inserted / 1000000.0 * progressMult
        if (progress > progressMult) progress = progressMult
        writeProgress(progress, inserted)
    }

    fun isValidCadNum(cadNum: String?): Boolean {
        if (cadNum == null) return false
        val regex = "\\d{2}:\\d{2}:(\\d{6,7}):\\d+".toRegex()
        val cleanedCadNum = cadNum.replace("[\"\\n.]".toRegex(), "").trim()
        return regex.matches(cleanedCadNum)
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

