package ru.samis.harvesters

import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringEscapeUtils
import org.bson.BsonArray
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.Document
import org.json.JSONArray
import org.json.JSONException
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.net.*
import java.nio.charset.Charset
import java.text.DateFormat
import java.util.*
import java.util.zip.GZIPInputStream


class CikParser : Harvester() {
    private val proxiesCollection = client.getDatabase(settings.getJSONObject("options").getString("proxiesDb"))
        .getCollection(settings.getJSONObject("options").getString("proxiesDataset"))
    private val proxies = proxiesCollection.find().toMutableList()
    //    private val startId = params.getJSONObject("params").getString("parentItem")
    private val regionName = StringEscapeUtils.unescapeJava(params.getString("region"))
    private val tmpCollection = db.getCollection("tmp_cik_houses")
    private val regionId = DigestUtils.sha1Hex(regionName)

    private val threads = mutableListOf<Thread>()
    override fun mainHarvest(): Int {
        var time = -System.nanoTime()
        insertMetadata(datasetStructure)

        println(regionName)
        var countryIndex: String
        while (true) {
            try {
                countryIndex = JSONArray(
                    downloadUrlToString(ADDRESS + "0")
                ).getJSONObject(0).getString("id")
                break
            } catch (e: Exception) {
            }
        }
        var regions: JSONArray
        while (true) {
            try {
                regions = JSONArray(downloadUrlToString(ADDRESS + countryIndex))
                break
            } catch (e: Exception) {
            }
        }
        var startId = ""
        for (i in 0 until regions.length()) {
            val region = regions.getJSONObject(i)
            if (region.getString("text") == regionName) {
                startId = region.getString("id")
                break
            }
        }
        if (startId.isEmpty()) {
            println("region not found")
            return -1
        }
        queue += AddrObj(startId, regionName, 2)

//        tmpCollection.drop()
        tmpCollection.createIndex(Indexes.ascending("id"))
        tmpCollection.createIndex(Indexes.ascending("level"))

        for (i in 0 until THREAD_COUNT) {
            threads += kotlin.concurrent.thread { thread() }
        }


        var count2 = tmpCollection.countDocuments()
        do {
            val count = count2
            println("${dateFormat.format(System.currentTimeMillis())} count $count, sleeping")
            Thread.sleep(60000)
            count2 = tmpCollection.countDocuments()
            println("${dateFormat.format(System.currentTimeMillis())} new count $count2")
            var progress = count2 / OBJECTS_COUNT * 0.8
            if (progress > 0.79) progress = 0.79
            writeProgress(progress, 0)
        } while (count2 > count || queue.isNotEmpty())

        for (thread in threads) {
            thread.interrupt()
        }

        time += System.nanoTime()
        println("loading time " + (time / 1e6).toInt())
        println("houses loaded ${housesCount.toInt()}")
        writeProgress(0.8, 0)

        cache.clear()
        housesCollection.drop()
        harvestFullAddresses(startId)
//        tmpCollection.drop()

        with(housesCollection) {
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("MunicipalitetID"))
            createIndex(Indexes.ascending("Municipalitet"))
            createIndex(Indexes.ascending("StreetID"))
            createIndex(Indexes.ascending("BlockID"))

            createIndex(
                Indexes.compoundIndex(
                    Indexes.text("Region"),
                    Indexes.text("Municipalitet"),
                    Indexes.text("Street"),
                    Indexes.text("HouseNumber")
                )
            )
        }
        return housesCollection.countDocuments().toInt()
    }

    private val cache = mutableMapOf<String, Document?>()

    private fun harvestFullAddresses(startId: String, parts: MutableMap<Int, Pair<String, String>> = mutableMapOf()) {
        val doc = cache.computeIfAbsent(startId) { tmpCollection.find(eq("id", startId)).first() } ?: return
        val level = doc.getInteger("level")
        parts[level] = startId to doc.getString("text").trim()

        val childrenCount = doc["children"]?.let {
            val list = it as List<String>
            for (id in list) {
                harvestFullAddresses(id, parts)
            }
            if (list.isEmpty()) {
                parts.remove(level)
                return
            }
            list.size
        } ?: 0


        val flatsCount = doc.getInteger("flatsCount") ?: 0
        val maxFlatNumber = doc.getInteger("maxFlatNumber") ?: 0
        if (level in 8..10 && flatsCount == childrenCount) {
            val nameList = arrayListOf<Pair<String, String>>()
//            var munId = ""
            var solid = 0
            parts[3]?.let {
                nameList += it
//                munId = it.first
            }
            parts[4]?.let {
                nameList += it
                solid = 1
//                munId = it.first
            }
            parts[13]?.let { nameList += it }
            parts[14]?.let { nameList += it }
            parts[5]?.let {
                nameList += it
                solid = 0
//                munId = it.first
            }
            parts[6]?.let {
                nameList += it
                solid = 0
//                munId = it.first
            }
            val municipality = nameList.joinToString(" ") { it.second }
            val district = parts[3] ?: parts[4] ?: parts[5] ?: parts[6] ?: "" to ""

            nameList.clear()
            parts[8]?.let { nameList += it }
            parts[9]?.let { nameList += it }
            parts[10]?.let { nameList += it }
            val houseNum = nameList.joinToString(" ") { it.second }

            housesCollection.insertOne(
                Document(
                    "ID",
                    DigestUtils.sha1Hex(regionName + municipality + district.second + parts[7]?.second + houseNum)
                )
                    .append("Region", regionName)
                    .append("RegionID", regionId)
                    .append("Municipalitet", municipality.replace("&quot;", "\""))
                    .append("MunicipalitetID", DigestUtils.sha1Hex(municipality))
//                    .append("MunDistrict", district.first)
                    .append("BlockID", district.second.replace("&quot;", "\""))
                    .append("Solid", solid)
                    .append("flatsCount", flatsCount)
                    .append("maxFlatNumber", maxFlatNumber)
                    .append("Street", parts[7]?.second?.replace("&quot;", "\""))
                    .append("StreetID", parts[7]?.let { DigestUtils.sha1Hex(municipality + it.second) })
                    .append("HouseNumber", houseNum.replace("&quot;", "\""))
                    .append("PostalCode", null)
                    .append("RegionCode", regionCode)
                    .append("x", null)
                    .append("y", null)
                    .append("Geometry", Document("type", "WTF").append("coordinates", null))
            )
            inserted++
            if (inserted % 10000 == 0) {
                var progress = 0.8 + 0.2 * inserted / housesCount
                if (progress > 0.99) progress = 0.99
                writeProgress(progress, inserted)
                println("$inserted houses inserted")
            }

            cache.remove(startId)
        }

        parts.remove(level)
    }

    private val queue = mutableListOf<AddrObj>()
    private val charset = Charset.forName("Windows-1251")
    private val mutex = Object()
    private val dateFormat = DateFormat.getTimeInstance()
    private var housesCount = 0.0
    private var inserted = 0


    private fun thread() {
        try {
            while (true) {
//                println("thread ${Thread.currentThread().id} iteration")
                var startObj: AddrObj
                synchronized(queue) {
                    //                    println("thread ${Thread.currentThread().id} queue locked")
                    while (queue.isEmpty()) {
//                        println("thread ${Thread.currentThread().id} queue empty, waiting")
                        (queue as Object).wait()
                    }
                    startObj = queue.last()
                    queue.removeAt(queue.lastIndex)
//                    println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} received object")
                }
                try {
                    println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} loading id=${startObj.id} text=${startObj.text}")
                    val json = downloadUrlToString(ADDRESS + startObj.id)
                    println("${dateFormat.format(System.currentTimeMillis())} loaded ${startObj.id} ${startObj.text}")
                    JSONArray(json.replace("\n", "")).apply {
                        val doc = Document("id", startObj.id).append("text", startObj.text)
                            .append("level", startObj.level)
                        var flatsCount = 0
                        var maxFlatNumber = 0
                        val ids = mutableListOf<BsonValue>()
//                        println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} ${length()} children")
                        for (i in 0 until length()) {
                            getJSONObject(i).apply {
                                val id = getString("id")
                                ids += BsonString(id)
                                val level = getJSONObject("a_attr").getString("levelid").toIntOrNull() ?: 0
//                                println("thread ${Thread.currentThread().id} level $level")
                                if (level in 8..10)
                                    synchronized(mutex) { housesCount++ }
                                if (level == 11) {
                                    flatsCount++
                                    val number = getString("text").toIntOrNull() ?: 0
                                    if (maxFlatNumber < number) maxFlatNumber = number
//                                    println("thread ${Thread.currentThread().id} new flat")
                                    tmpCollection.insertOne(
                                        Document("id", id).append("text", getString("text"))
                                            .append("level", 11)
                                    )
                                } else
                                    synchronized(queue) {
                                        queue += AddrObj(
                                            id,
                                            getString("text"),
                                            level
                                        )
//                                        println("thread ${Thread.currentThread().id} added to queue")
                                        (queue as Object).notifyAll()
                                    }
                            }
                        }
                        if (ids.isNotEmpty())
                            doc.append("children", BsonArray(ids))
                        if (flatsCount > 0) {
                            doc.append("flatsCount", flatsCount)
                            doc.append("maxFlatNumber", maxFlatNumber)
                        }
//                        println("thread ${Thread.currentThread().id} inserting")
                        tmpCollection.insertOne(doc)
                    }
                } catch (e: JSONException) {
                    e.printStackTrace()
                    println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} JSON parse error id=${startObj.id} text=${startObj.text}")
                } catch (e: Exception) {
                    e.printStackTrace()
                    println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} error id=${startObj.id} text=${startObj.text}")
                    synchronized(queue) {
                        println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} locked queue, re-adding")
                        queue += startObj
                        (queue as Object).notifyAll()
                    }
                }
            }
        } catch (e: InterruptedException) {

        }
    }

    class AddrObj(var id: String, var text: String, var level: Int) {
        override fun toString(): String {
            return "$id $text"
        }
    }


    @Throws(IOException::class)
    fun downloadUrlToString(address: String): String {
        val out = ByteArrayOutputStream()
        downloadUrlToStream(address, out)
        out.close()

        return String(out.toByteArray(), CHARSET)
    }



    @Throws(IOException::class)
    fun downloadUrlToStream(
        address: String,
        out: OutputStream? = null
    ) {
        val connection: URLConnection
        val url: URL
        val httpConnection: HttpURLConnection

        var cookies: String? = null
        val proxyDoc = proxies.random()
        val proxy = proxyDoc?.let {
            val addrPort = proxyDoc.getString("url").split(":")

            val addr = InetSocketAddress(addrPort[0], addrPort[1].toInt())
            val typeStr = proxyDoc.getString("type").toLowerCase()
            val type = when {
                typeStr.startsWith("socks") -> Proxy.Type.SOCKS
                typeStr.startsWith("http") -> Proxy.Type.HTTP
                else -> Proxy.Type.DIRECT
            }

            val loginPass = proxyDoc.getString("auth").split(":")

            val authenticator = object : Authenticator() {
                override fun getPasswordAuthentication() =
                    PasswordAuthentication(loginPass[0], loginPass[1].toCharArray())
            }
            Authenticator.setDefault(authenticator)

            cookies = proxyDoc.getString("cookies")
            Proxy(type, addr)
        }

        try {
            url = URL(address)
            connection = if (proxy != null) url.openConnection(proxy) else url.openConnection()
            httpConnection = connection as HttpURLConnection
            httpConnection.requestMethod = "GET"
            httpConnection.doOutput = false


            httpConnection.setRequestProperty("Accept-Encoding", "gzip, deflate")
            httpConnection.setRequestProperty(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${50 + (Math.random() * 49).toInt()}.0.${100 + (Math.random() * 3356).toInt()}.51 Safari/537.36"
            )
            httpConnection.setRequestProperty("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
            httpConnection.setRequestProperty("Cache-Control", "no-cache")
            httpConnection.setRequestProperty("Host", "cikrf.ru")
            httpConnection.setRequestProperty("Connection", "keep-alive")
            httpConnection.setRequestProperty(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            )
            httpConnection.setRequestProperty("Pragma", "no-cache")
            httpConnection.setRequestProperty("Upgrade-Insecure-Requests", "1")

            cookies?.let {
                httpConnection.setRequestProperty("Cookie", cookies)
            }
            httpConnection.setRequestProperty("DNT", "1")
            httpConnection.setRequestProperty("Sec-GPC", "1")
//                httpConnection.setRequestProperty("", "")
            httpConnection.doInput = true


            httpConnection.connectTimeout = 3000
            httpConnection.readTimeout = 3000

            httpConnection.connect()

            val responseCode = httpConnection.responseCode
            if (responseCode == HttpURLConnection.HTTP_OK) {
                proxyDoc?.run {
                    httpConnection.getHeaderField("Set-Cookie")?.let {
                        try {
                            for (cookie in it.split("; ")) {
                                if (cookie.startsWith("session-cookie")) {
                                    proxyDoc["cookies"] = cookie
                                    proxiesCollection.updateOne(
                                        eq("url", proxyDoc["url"]),
                                        Updates.set("cookies", cookie)
                                    )
                                    println("${dateFormat.format(System.currentTimeMillis())} thread ${Thread.currentThread().id} $cookie")
                                    break
                                }
                            }
                        } catch (e: Exception) {
                        }
                    }
                }

                var inputStream = httpConnection.inputStream

                val byteArrayOutputStream = ByteArrayOutputStream()
                inputStream.copyTo(byteArrayOutputStream, 1024)
                inputStream.close()
                val read = byteArrayOutputStream.toByteArray()
                inputStream = ByteArrayInputStream(read)

                if ("gzip" == httpConnection.contentEncoding) {
                    inputStream = GZIPInputStream(inputStream)
                }

                val buffer = ByteArray(1024)
                var c = inputStream.read(buffer)
                var i = 0
                while (c > -1) {
                    out?.write(buffer, 0, c)
                    i++
                    c = inputStream.read(buffer)
                }

                inputStream.close()
                httpConnection.disconnect()
            } else {
                throw IOException("HTTP Error: code $responseCode")
            }
        } catch (ex: IOException) {
//            ex.printStackTrace()
            proxyDoc?.let {
                proxyDoc["cookies"] = null
                proxiesCollection.updateOne(
                    eq("url", proxyDoc["url"]),
                    Updates.set("cookies", null)
                )
            }
            throw ex
        }
    }


    companion object {
        private val ADDRESS = "http://cikrf.ru/services/lk_tree/?id="
        private val THREAD_COUNT = 3
        private val OBJECTS_COUNT = 2000000.03
        private val CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
        private val CHARSET = Charset.forName("Windows-1251")


        private val datasetStructure = arrayOf(
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