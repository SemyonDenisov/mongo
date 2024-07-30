package ru.samis.harvesters

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.bson.types.ObjectId
import org.locationtech.proj4j.CoordinateTransformFactory
import org.locationtech.proj4j.ProjCoordinate
import java.io.File
import java.io.FileReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.util.*
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.thread

class MsHarvester : FragmentationHarvester() {
    private val regionName = params.getString("region")
    private val regionId = DigestUtils.sha1Hex(regionName)


    override fun mainHarvest(): Int {
        insertMetadata(datasetStructure)

        writeProgress(0.0, 0)

        housesCollection.drop()

        housesCollection.apply {
            createIndex(Indexes.ascending("x"))
            createIndex(Indexes.ascending("y"))
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("MunicipalitetID"))
            createIndex(Indexes.ascending("Municipalitet"))
            createIndex(Indexes.ascending("StreetID"))
            createIndex(Indexes.ascending("Street"))
            createIndex(Indexes.ascending("HouseNumber"))
            createIndex(Indexes.ascending("BlockID"))
            createIndex(
                Indexes.compoundIndex(
                    Indexes.text("Region"),
                    Indexes.text("Municipalitet"),
                    Indexes.text("Street"),
                    Indexes.text("HouseNumber")
                )
            )
            createIndex(
                Indexes.compoundIndex(
                    Indexes.ascending("Municipalitet"),
                    Indexes.ascending("Street"),
                    Indexes.ascending("HouseNumber")
                )
            )
        }

        val crsFactory = org.locationtech.proj4j.CRSFactory()
        val wgs84 = crsFactory.createFromName("epsg:4326")
        val merc = crsFactory.createFromName("epsg:3857")
        val ctFactory = CoordinateTransformFactory()
        val wgsToMerc = ctFactory.createTransform(wgs84, merc)

        val result = ProjCoordinate()

        val fullDataset = client
            .getDatabase(params.getString("fullDatasetDb")).getCollection(params.getString("fullDataset"))

        val regionBorder = regionsCollection.find(eq("_id", ObjectId(regionBorderId))).first()["geometry"] as Document
        val minMax = arrayOf(180.0, 180.0, -180.0, -180.0)
        findMinMax(regionBorder["coordinates"] as List<*>, minMax)

        minMax[0] = lon2merc(minMax[0])
        minMax[2] = lon2merc(minMax[2])
        minMax[1] = lat2merc(minMax[1])
        minMax[3] = lat2merc(minMax[3])

        var time = -System.nanoTime()
        val housesInBbox = fullDataset.find(
            and(
                gt("x", minMax[0]),
                lt("x", minMax[2]),
                gt("y", minMax[1]),
                lt("y", minMax[3])
            )
        )
        val count = housesInBbox.count()
        time += System.nanoTime()
        println("found $count houses in bbox, query took ${time / 1e6} ms")

        var counter = 0
        val tmpFile = File("tmp.geojsonl.gz")
        OutputStreamWriter(GZIPOutputStream(tmpFile.outputStream().buffered())).use {
            for (doc in housesInBbox) {
                doc["Geometry"] ?: continue
                it.write(doc.toJson())
                it.write("\n")
                counter++

                if (counter % 1000 == 0) println("wrote $counter")
            }
        }

        counter = 0
        var skipped = 0
        InputStreamReader(GZIPInputStream(tmpFile.inputStream().buffered())).use {
            it.forEachLine { line ->
                val doc = Document.parse(line)
                try {
                    val geometry = doc["Geometry"] as Document
                    regionMunicipalicies.filter(
                        geoIntersects("geometry", geometry)
                    ).first()?.let { municipality ->
                        if (municipality["region"] != regionName) {
                            skipped++
                            if (skipped % 1000 == 0) println("skipped $skipped")
                            return@let
                        }
                        val centroid = centroid(geometry.toJson())

                        wgsToMerc.transform(
                            ProjCoordinate(centroid.x, centroid.y),
                            result
                        )

                        doc["x"] = result.x
                        doc["y"] = result.y
                        doc.append(
                            "radius",
                            avgRaduis(
                                geometry["coordinates"] as List<*>,
                                result.x,
                                result.y
                            )
                        )
                        doc.append("trueGeometry", trueGeometry(geometry["coordinates"] as List<*>))
                        municipality["name"]?.apply {
                            doc["Municipalitet"] = this
                            doc["BlockID"] = this
                            doc["MunicipalitetID"] = DigestUtils.sha1Hex(toString())
                        }
                        doc["Region"] = regionName
                        doc["RegionID"] = regionId
                        doc["RegionCode"] = regionCode
                        doc.remove("_id")
                        housesCollection.insertOne(doc)

                        counter++

                        if (counter % 1000 == 0) {
                            println("inserted $counter")
                            writeProgress(1.0 * counter / count, counter)
                        }
                    }
                } catch (e: Exception) {
                    println(e.message)
                    println(doc)
                }
            }
        }

        println("inserted $counter")

        tmpFile.delete()
        return counter
    }

    private fun findMinMax(coords: List<*>, minMax: Array<Double>) {
        if (coords[0] is List<*>) {
            for (subList in coords) {
                findMinMax(subList as List<*>, minMax)
            }
            return
        }
        var coord1 = minMax[0]
        var coord2 = minMax[1]
        if (coords[0] is Int) {
            coord1 = (coords[0] as Int).toDouble()
        }
        if (coords[1] is Int) {
            coord2 = (coords[1] as Int).toDouble()
        }
        if (coords[0] is Double) {
            coord1 = coords[0] as Double
        }
        if (coords[1] is Double) {
            coord2 = coords[1] as Double
        }

        if (coord1 < minMax[0]) minMax[0] = coord1
        if (coord1 > minMax[2]) minMax[2] = coord1

        if (coord2 < minMax[1]) minMax[1] = coord2
        if (coord2 > minMax[3]) minMax[3] = coord2

    }

    private fun parseGeoJSON(geojson: String): Document? {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(geojson)

        var doc = Document()
        var counter = 0
        var geomCounter = 0
        var geomPointsCounter = 0
        val geomHierarchy = mutableListOf<MutableList<Any>>()
        var geometryStarted = false
        var proc: (() -> Unit)? = null
        var lat = 0.0
        var lon = 0.0
        var inProps = false

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
                val name = parser.currentName
                when (name.uppercase(Locale.getDefault())) {
                    "TYPE" -> {
                        proc = {
                            if (parser.valueAsString == "Feature") {
                                geometryStarted = false
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
                                    .append("RegionCode", regionCode)
                                counter = 0
                            }
                        }
                    }

                    "PROPERTIES" -> {
                        proc = { inProps = true }
                    }

                    "GEOMETRY" -> {
                        geometryStarted = true
                        geomCounter = 0
                        geomPointsCounter = 0
                        geomHierarchy.clear()
                        lat = 0.0
                        lon = 0.0
                    }

                    else -> {
                        if (inProps) {
                            val value = try {
                                parser.valueAsString
                            } catch (e: Exception) {
                                try {
                                    parser.valueAsInt
                                } catch (e: Exception) {
                                    try {
                                        parser.valueAsDouble
                                    } catch (e: Exception) {
                                        null
                                    }
                                }
                            }

                            proc = { doc.append(name, value) }
                        }
                    }
                }
            }

            if (token == JsonToken.START_OBJECT) counter++
            if (token == JsonToken.END_OBJECT) {
                counter--
                inProps = false
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

//                        if (!isInRegion(lat, lon)) return null

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
//                            .append("BlockID", findArea(lat, lon))
                            .append("ID", "$lat$lon")
                        doc.append("trueGeometry", trueGeometry(geomHierarchy[0]))
                    }
                }
            }

            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if (geomCounter > 0) {
                    geomHierarchy.last().add(parser.valueAsDouble)
                }
            }
        }

        return doc
    }

    fun harvestFullFile() {
        val dir = settings.getJSONObject("options").getString("catalog")

        var readCount = 0
        var insertedCount = 0

        var count = 65592759

        val fileName = params.optString("")
        val file = File(dir + fileName)

        val queue = mutableListOf<String>()
        val threads = mutableListOf<Thread>()

        for (i in 0 until THREADS) {
            threads += thread {
                val client = MongoClients
                    .create(params.getString("ConnectionString"))
                val db: MongoDatabase = client
                    .getDatabase(params.getString("database"))
                val housesCollection: MongoCollection<Document> = db.getCollection(params.getString("dataset"))

                while (true) {
                    if (Thread.currentThread().isInterrupted) {
                        println("Thread #${Thread.currentThread().id} interrupted")
                        break
                    }
                    var doc: Document?
                    synchronized(queue) {
                        while (queue.isEmpty()) {
                            if (Thread.currentThread().isInterrupted) {
                                println("Thread #${Thread.currentThread().id} interrupted")
                                break
                            }
//                            println("Thread #${Thread.currentThread().id} waiting for data")
                            (queue as Object).wait()
                        }
                        doc = parseGeoJSON(queue.last())
                        queue.removeAt(queue.lastIndex)
                        (queue as Object).notifyAll()
                    }

//                    println("Thread #${Thread.currentThread().id} received data")
                    doc?.let {
                        housesCollection.insertOne(it)
                    }


                }
            }
        }


        thread {
            FileReader(file).buffered().apply {
                synchronized(queue) {
                    //                    for (i in 0 until 11484196) {
//                        readLine()
//                        if (i % 100000 == 0) println("$i skipped")
//                    }
                    while (true) {
                        if (queue.isEmpty()) {
//                            println("Reader reading data")
                            for (i in 0 until THREADS) {
                                val geojson = readLine() ?: break

                                queue += geojson
                                insertedCount++

                                readCount++
                                if (readCount % 1000 == 0) {
                                    val progress = 1.0 * readCount / count
                                    println("$readCount / $count inserted $insertedCount")
                                    writeProgress(progress, insertedCount)
                                }
                            }
                            if (queue.isEmpty()) {
                                println("No data read")
                                for (thread in threads) {
                                    thread.interrupt()
                                }
                                threads.clear()
                                (queue as Object).notifyAll()
                                break
                            }
                            (queue as Object).notifyAll()
                        } else {
//                            println("Reader not need to read")
                            (queue as Object).wait()
                        }
                    }
                }
            }
            writeComplete(readCount)
        }
    }

    companion object {
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
        private val THREADS = 20
    }
}
