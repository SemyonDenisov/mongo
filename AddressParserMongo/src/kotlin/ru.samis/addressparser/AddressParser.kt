package ru.samis.addressparser

import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.json.JSONObject
import java.io.File
import java.io.LineNumberReader
import java.nio.charset.Charset
import kotlin.concurrent.thread
import kotlin.math.ceil
import kotlin.math.max

open class AddressParser {
    private val dataset: MongoCollection<Document>
    private val addressFields: MutableList<String>
    private val addressFieldsOld: MutableList<String?>
    private val keyField: String
    private val filter: Document
    private var statuses = mutableMapOf<String, Boolean>()
    private val settings by lazy {
        JSONObject(File("settings.json").readText())
    }
    private val region: String
    private val updateCollection: Boolean


    constructor() {
        val params = settings.getJSONObject("params")

        val db = MongoClients.create(params.getString("ConnectionString"))
            .getDatabase(params.getString("database"))
        this.dataset = db.getCollection(params.getString("dataset"))
        addressFields = mutableListOf(params.optString("addressField", "AddressDesc"))
        keyField = params.optString("keyField", "cadNumInt")
        addressFieldsOld = mutableListOf()

        val filtersJSON = params.optString("filter")
        this.filter = if (filtersJSON.isNotEmpty()) Document.parse(filtersJSON) else Document()
        // TODO
        this.region = MongoClients.create(params.getString("ConnectionString"))
            .getDatabase("rk_metadata")
            .getCollection("regionState")
            .find(Document("database", params.getString("database"))).first()
            ?.let { document -> document.getString("region") ?: "" } ?: ""
        this.updateCollection = params.optBoolean("updateCollection", true)

    }

    constructor(
        database: String,
        dataset: String,
        addressFields: MutableList<String>,
        addressFieldsOld: MutableList<String?>,
        keyField: String,
        filter: Document
    ) {
        val settings = JSONObject(File("settings.json").readText())
        val params = settings.getJSONObject("params")

        println("database $database")
        println("dataset $dataset")
        val db = MongoClients.create(params.getString("ConnectionString"))
            .getDatabase(database)
        this.dataset = db.getCollection(dataset)
        this.addressFields = addressFields
        this.addressFieldsOld = addressFieldsOld
        this.keyField = keyField
        this.filter = filter
        // TODO
        this.region = MongoClients.create(params.getString("ConnectionString"))
            .getDatabase("rk_metadata")
            .getCollection("regionState")
            .find(Document("database", params.getString("database"))).first()
            ?.let { document -> document.getString("region") ?: "" } ?: ""
        this.updateCollection = params.optBoolean("updateCollection", true)

    }

    private val ALLOWED_CHARS = hashSetOf('/', ',', '.', '\\', '-', ' ')
    private fun filterAddress(address: String): String {
        val result = StringBuilder()
        for (c in address) {
            if (c.isLetterOrDigit() || ALLOWED_CHARS.contains(c))
                result.append(c)
            else
                result.append(" ")
        }

        var resultStr = result.toString()
        do {
            val len = resultStr.length
            resultStr = resultStr.replace("  ", " ")
        } while (len != resultStr.length)

        return resultStr
    }

    fun parseAddresses(progressCallback: (Int, Int) -> Unit = { _, _ -> }): Int {
        var time = -System.nanoTime()
        dataset.createIndex(Indexes.ascending(keyField))

        val conditions = mutableListOf<Bson>()
        for (field in addressFields) {
            conditions += Filters.and(Filters.ne(field, null), Filters.ne(field, ""))
        }

        val found = dataset.find(Filters.and(Filters.or(conditions), filter)).skip(0)

        val totalCount = found.count()
        println("$totalCount addresses for recognize")

        val threadsCount = settings.getJSONObject("options").optInt("threads", 1)
        val bucketSize = max(ceil(1.0 * totalCount / threadsCount).toInt(), 100)
        println("$threadsCount threads")
        println("bucketSize for each thread $bucketSize")
        var bucketNumber = 0
        var writer = File("bucket$bucketNumber.txt").writer(Charset.forName("windows-1251"))
        for ((i, document) in found.withIndex()) {
            if (i - bucketNumber * bucketSize == bucketSize) {
                bucketNumber++
                writer.close()
                writer = File("bucket$bucketNumber.txt").writer(Charset.forName("windows-1251"))
            }

            val addr = StringBuilder()
            for (field in addressFields) {
                document.getString(field)?.let { addr.append(it).append(" ") }
            }
            var address = addr.trim().toString()
            if (address.isBlank()) continue
            address = filterAddress(address)

            ///////////////
            if (address.isBlank()) continue
            writer.write(JSONObject().put("address", address).put(keyField, document[keyField]).toString())
            writer.write("\n")
        }
        writer.close()

        statuses.clear()
        for (i in 0..bucketNumber) {
            val file = "bucket$i.txt"
            statuses[file] = false
            thread {
                parseBucket(file)
//                File(file).delete()

                synchronized(statuses) {
                    statuses[file] = true
                    (statuses as Object).notifyAll()
                }
            }
        }

        while (statuses.any { !it.value }) {
            println("AddressParser sleeping")
            synchronized(statuses) {
                (statuses as Object).wait()
            }
        }

        time += System.nanoTime()
        println("total time " + (time / 1e6).toInt())
        return totalCount
    }

    private fun parseBucket(bucketFile: String) {

        val outFile = "${bucketFile}_united_out.jsonl"
        UnitedParser().parse(bucketFile, outFile)

        println("UnitedParser for $bucketFile finished")
        LineNumberReader(
            File(outFile)
                .reader(Charset.forName("windows-1251"))
        ).use { reader ->
            reader.forEachLine { line ->
                try {
//                    println("line $line")
                    val json = JSONObject(line)
                    val key = if (keyField == "_id") ObjectId(json.optString(keyField)) else json[keyField]

//                    println("$keyField $key")
                    val doc = dataset.find(Filters.eq(keyField, key)).first()!!

                    if (addressFieldsOld.size == 4) {
                        for ((i, field) in addressFieldsOld.withIndex()) {
                            field ?: continue
                            doc[addressFields[i]]?.let { doc[field] = it }
                            doc.remove(addressFields[i])
                        }
                    }

                    val block = "${json.opt("district_type")} ${json.opt("district")}".trim()
                    val municipality = "${json.opt("city_type")} ${json.opt("city")}".trim()
                    //val region = "${json.opt("region")} ${json.opt("region_type")}".trim()
                    val street = "${json.opt("street_type")} ${json.opt("street")}".trim()
                    doc["Region"] = region
                    doc["Municipalitet"] = municipality
                    doc["District"] = block
                    doc["Street"] = street
                    doc["HouseNumber"] = "${json.opt("house_type")} ${json.opt("house")}".trim()
                    doc["Flat"] = "${json.opt("unit_type")} ${json.opt("unit")}".trim()

                    doc["RegionID"] = DigestUtils.sha1Hex(region)
                    //doc["BlockID"] = DigestUtils.sha1Hex(block)
                    //
                    doc["MunicipalitetID"] = DigestUtils.sha1Hex(region + municipality)
                    doc["StreetID"] = DigestUtils.sha1Hex(region + municipality + street)

//                    println("doc $doc")

                    if (updateCollection) {
                        dataset.replaceOne(
                            Filters.eq(keyField, key),
                            doc
                        )
                    }

                } catch (e: Exception) {
                    println("exception in united result processing for $bucketFile ${e.localizedMessage}")
                    println("line $line")
                    e.printStackTrace()
                }
            }
        }
    }

    companion object {
        val PORTION_SIZE = 100


    }
}