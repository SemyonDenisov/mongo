package ru.samis.harvesters

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.util.*
import kotlin.concurrent.thread

class MsPrepare : FragmentationHarvester() {
    private val regionName = params.getString("region")


    override fun mainHarvest(): Int {

        val dir = settings.getJSONObject("options").getString("catalog")

        var readCount = 0
        var insertedCount = 0

        var count = 65592759

        val fileName = params.optString("")
        val file = File(dir + fileName)
        val files = mutableMapOf<String, FileWriter>()

        val queue = mutableListOf<String>()
        val threads = mutableListOf<Thread>()

        for (i in 0 until THREADS) {
            threads += thread {
                while (true) {
                    if (Thread.currentThread().isInterrupted) {
                        println("Thread #${Thread.currentThread().id} interrupted")
                        break
                    }
                    var line = ""
                    synchronized(queue) {
                        while (queue.isEmpty()) {
                            if (Thread.currentThread().isInterrupted) {
                                println("Thread #${Thread.currentThread().id} interrupted")
                                break
                            }
                            println("Thread #${Thread.currentThread().id} waiting for data")
                            (queue as java.lang.Object).wait()
                        }
                        line = queue.last()
                        queue.removeAt(queue.lastIndex)
                        (queue as java.lang.Object).notifyAll()
                    }

                    println("Thread #${Thread.currentThread().id} received data")
                    val region = findRegionFromGeoJSON(line)
                    val writer = files.getOrPut(region, { FileWriter("$dir$region.geojsonl") })
                    writer.write(line)
                    writer.write("\n")
                    writer.flush()

                    readCount++
                    if (readCount % 100 == 0) {
                        val progress = 1.0 * readCount / count
                        println("$readCount / $count inserted $insertedCount")
                        writeProgress(progress, insertedCount)
                    }
                }
            }
        }


        thread {
            FileReader(file).buffered().apply {
                synchronized(queue) {
                    while (true) {
                        if (queue.isEmpty()) {
                            println("Reader reading data")
                            for (i in 0 until THREADS) {
                                val geojson = readLine() ?: break
                                queue += geojson
                            }
                            if (queue.isEmpty()) {
                                println("No data read")
                                for (thread in threads) {
                                    thread.interrupt()
                                }
                                threads.clear()
                                (queue as java.lang.Object).notifyAll()
                                break
                            }
                            (queue as java.lang.Object).notifyAll()
                        } else {
                            println("Reader not need to read")
                            (queue as java.lang.Object).wait()
                        }
                    }
                }
            }
        }

        for (writer in files.values) {
            writer.close()
        }

        return readCount
    }

//    override val regionsCollection = MongoClients
//        .create(params.getString("ConnectionString"))
//        .getDatabase(regionBorderDb)
//        .getCollection(regionBorderDataset)

    private fun findRegionFromGeoJSON(geojson: String): String {
        val jsonFactory = JsonFactory()
        val parser = jsonFactory.createParser(geojson)

        var counter = 0
        var geomCounter = 0
        val geomHierarchy = mutableListOf<MutableList<Any>>()
        var geometryStarted = false
        var proc: (() -> Unit)? = null

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

                                counter = 0
                            }
                        }
                    }

                    "GEOMETRY" -> {
                        geometryStarted = true
                        geomCounter = 0
                        geomHierarchy.clear()
                    }


                }
            }

            if (token == JsonToken.START_OBJECT) counter++
            if (token == JsonToken.END_OBJECT) {
                counter--

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
                                return findRegion(
                                    point[1], point[0], regionsCollection
                                )
                            }

                        }
                        geomHierarchy.removeAt(geomHierarchy.lastIndex)
                    }
                    if (geomCounter == 0) {
                        geometryStarted = false


                    }
                }
            }

            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if (geomCounter > 0) {
                    geomHierarchy.last().add(parser.valueAsDouble)
                }
            }
        }

        return ""
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
        private val THREADS = 100
    }
}
