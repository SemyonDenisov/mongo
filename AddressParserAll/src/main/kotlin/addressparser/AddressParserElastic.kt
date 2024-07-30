package ru.samis.addressparser

import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import org.apache.http.HttpHost
import org.bson.Document
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.json.JSONArray
import org.json.JSONObject
import java.io.*
import java.net.InetAddress
import java.util.*
import kotlin.system.exitProcess

class AddressParserElastic(
    datasetPath: String,
    parserDir: String,
    outFile: String,
    elasticHost: String,
    elasticPort: Int,
    elasticScheme: String
) : AddressParserBase(parserDir, datasetPath, outFile) {

    protected val client = MongoClients
        .create(options.getString("ConnectionString"))

    private val regionsByName = mutableMapOf<String, Region>()
    private val regionsByFias = mutableMapOf<String, Region>()
    private var settlementsByRegFias = mutableMapOf<String, MutableList<SettlementCluster>>()
    private var allSettlements = mutableMapOf<String, SettlementCluster>()
    private var streets = mutableMapOf<String, MutableMap<String, MutableList<Street>>>()

    private val cleanTemp = options.optBoolean("cleanTempElastic", true)

    private val datasetCsvFileName = "dataset${System.nanoTime()}.csv"

    override val externalCommand: String = "python3 ${parserDir}parse_dataset.py $datasetCsvFileName"

    init {
        builder = ProcessBuilder(listOf("sh", "-c", externalCommand)).directory(File(parserDir))
    }

    private lateinit var parsedAddresses: Array<Address>
    private val elasticClient =
        RestHighLevelClient(RestClient.builder(HttpHost(elasticHost, elasticPort, elasticScheme)))
    private val searchRequest = SearchRequest("fias_full_text")

    override fun postProcess(result: JSONObject) {
        var region = result.optString("region")
        val startIndex = region.indexOf("(")
        if (startIndex >= 0) {
            val endIndex = region.indexOf(")", startIndex)
            if (endIndex >= 0)
                result.put("region", region.replaceRange(startIndex..endIndex, ""))
        }

        super.postProcess(result)

        region = result.optString("region")
        val address = result.optString("address")
        if (!address.contains(region))
            result.put("region", "")
    }

    private fun ownParse(address: String): Address {
        var address = address.lowercase(Locale.getDefault())
            .replace(" -", "-")
            .replace("- ", "-")
            .replace(" )", ")")
            .replace("( ", "(")
            .replace('.', ' ')
            .replace(',', ' ')
        do {
            val len = address.length
            address = address.replace("  ", " ")
        } while (len > address.length)
        val result = Address()
        var regionIndex = -1
        for (region in regionsByName.values) {
            for (variant in region.variants) {
                regionIndex = findWord(address, variant)
                if (regionIndex >= 0) {
                    result.region = region
                    break
                }
            }
        }

        val settl = if (result.region == null)
            allSettlements.values
        else
            settlementsByRegFias.getOrDefault(result.region!!.fias, mutableListOf())

        val found = mutableListOf<SettlementCluster>()
        val foundIndexes = mutableListOf<Int>()
        for (settlement in settl) {
            val index = findWord(address, settlement.nameLC, regionIndex + 1)
            if (index < 0) continue
//            if (found.isEmpty()) {
            found += settlement
            foundIndexes += index
//            } else if (foundIndexes[0] > index) {
//                found[0] = settlement
//                foundIndexes[0] = index
//            }
        }


        val regionCandidates = mutableListOf<Region>()
        val settlCandidates = mutableListOf<SettlementCluster>()
        val streetCandidates = mutableListOf<Street>()
        val indexes = mutableListOf<Int>()
        for ((i, settlCandidate) in found.withIndex()) {
            val regions = if (result.region != null)
                listOf(result.region!!)
            else {
                mutableListOf<Region>().also { listRegions ->
                    for (settlement in settlCandidate.settlements) {
                        val regionFias = settlement.regionFias ?: continue
                        listRegions += regionsByFias[regionFias] ?: continue
//                    val region = regions.find { region ->
//                        val settlRegion = settlement.region.toLowerCase()
//                        settlRegion.contains(region.nameLC) || region.nameLC.contains(settlRegion)
//                    } ?: continue
//                    listRegions += region
                    }
                }
            }

            val settlIndex = foundIndexes[i]
            for (region in regions) {
                val regionStreets = streets[region.name] ?: continue
                val settlStreets = regionStreets[settlCandidate.name] ?: continue
                for (street in settlStreets) {
                    val streetIndex = findWord(address, street.name, settlIndex + 1)
                    if (streetIndex < 0) continue

                    regionCandidates += region
                    settlCandidates += settlCandidate
                    streetCandidates += street
                    indexes += i
                }
            }
        }

        if (regionCandidates.isNotEmpty()) {
            var minStrIndex = Int.MAX_VALUE
            var indexOfMin = Int.MAX_VALUE
            for ((i, index) in indexes.withIndex()) {
                if (minStrIndex > foundIndexes[index]) {
                    minStrIndex = foundIndexes[index]
                    indexOfMin = i
                }
            }
//            result.region = regionCandidates[indexOfMin]
            result.settlement = settlCandidates[indexOfMin].name
            result.street = streetCandidates[indexOfMin].name
        } else if (found.isNotEmpty()) {
            var minStrIndex = Int.MAX_VALUE
            var indexOfMin = Int.MAX_VALUE
            for ((i, index) in foundIndexes.withIndex()) {
                if (minStrIndex > index) {
                    minStrIndex = index
                    indexOfMin = i
                }
            }

            val settlement = found[indexOfMin]
            result.settlement = settlement.name
//            if (settlement.settlements.size == 1 && result.region == null) {
//                result.region = regionsByFias[settlement.settlements[0].regionFias]
//            }
        }

        return result
    }

    override fun init() {
        super.init()
        val regionsJson = JSONArray(File("regions.json").readText())
        for (i in 0 until regionsJson.length()) {
            val regionJson = regionsJson.getJSONObject(i)

            Region(
                regionJson.getString("name"),
                regionJson.optString("otherName"),
                regionJson.optString("fiasName"),
                regionJson.optString("typeShort").lowercase(Locale.getDefault()),
                regionJson.getJSONArray("typeLong").let { typeLong ->
                    Array(typeLong.length()) { index -> typeLong.getString(index).lowercase(Locale.getDefault()) }
                },
                regionJson.getString("fias"),
                regionJson.optBoolean("nameSufficient")
            ).apply {
                regionsByName += nameLC to this
                regionsByFias += fias to this
            }
        }

//        readAddressPartsFromDb()

        var time = -System.nanoTime()
        ObjectInputStream(FileInputStream("streets.bin")).use {
            streets = it.readObject() as MutableMap<String, MutableMap<String, MutableList<Street>>>
        }

//        ObjectInputStream(FileInputStream("allSettlements.bin")).use {
//            allSettlements = it.readObject() as MutableMap<String, SettlementCluster>
//        }

        ObjectInputStream(FileInputStream("settlements.bin")).use {
            settlementsByRegFias = it.readObject() as MutableMap<String, MutableList<SettlementCluster>>
        }

        for (clusters in settlementsByRegFias.values) {
            for (cluster in clusters) {
                allSettlements[cluster.name] = cluster
            }
        }

        time += System.nanoTime()
        println("Elastic ${File(datasetPath).name} read bins ${time / 1e9}")

//        File("settlements.jsonl").bufferedReader().useLines { seq ->
//            for (line in seq) {
//                with(JSONObject(line)) {
//                    val name = getString("name")
//                    with(getJSONArray("regions")) {
//                        var region = ""
//                        for (i in 0 until length()) {
////                            println(getString(i))
//                            region = getString(i)
//                            if (region.isBlank()) continue
//                            val regionSettl = settlementsByRegFias.getOrPut(region.toLowerCase(), { mutableListOf() })
//                            regionSettl += Settlement(name, region)
//                        }
//                        allSettlements += Settlement(
//                            name,
//                            if (length() == 1) region else ""
//                        )
//                    }
//                }
//            }
//        }
    }

    fun readAddressPartsFromDb() {
        val allDatabases = options.getJSONArray("allDatabases")

        for (i in 0 until allDatabases.length()) {
            val db = client.getDatabase(allDatabases.getString(i))
            println(allDatabases.getString(i))
            val streetsColl = db.getCollection("gar_streets")

            val streetsResult = streetsColl
                .find(Filters.`in`("LEVEL", "7", "8"))
                .sort(Document("Municipalitet", 1))
            var region: String? = null
            var regionFias: String? = null
            var currSettl = ""
            var regionStreets = mutableMapOf<String, MutableList<Street>>()
            var settlStreets = mutableListOf<Street>()
            for (street in streetsResult) {
                if (region == null) {
                    region = street.getString("Region") ?: continue
                    regionFias = street.getString("RegionID") ?: continue
                    region = NAME_CORRECTION[region] ?: region
                    regionStreets = streets.getOrPut(region, { mutableMapOf() })
                }
                val settl = street.getString("Municipalitet") ?: continue
                if (settl != currSettl) {
                    currSettl = settl
//                    print("\t")
//                    println(settl)
                    settlStreets = regionStreets.getOrPut(settl, { mutableListOf() })
                }
//                print("\t\t")
//                println(street.getString("NAME"))
                settlStreets.add(
                    Street(
                        street.getString("NAME"),
                        street.getString("TYPENAME"),
                        street.getString("LEVEL").toInt(),
                        street.getString("Territory")
                    )
                )
            }

            val regionSettl = settlementsByRegFias.getOrPut(regionFias!!, { mutableListOf() })
            val settlementColl = db.getCollection("gar_settlements")
            val settlResults = settlementColl
                .find(
                    Filters.or(
                        Filters.eq("LEVEL", "4"),
                        Filters.eq("LEVEL", "5"),
                        Filters.eq("LEVEL", "6")
                    )
                )
                .sort(Document("NAME", 1))

            var currCluster = SettlementCluster("")
            for (result in settlResults) {
                val name = result.getString("NAME")
                if (name != currCluster.name) {
                    currCluster = allSettlements.getOrPut(name) { SettlementCluster(name) }
                    regionSettl.add(currCluster)
                }

                currCluster.settlements += Settlement(
                    name,
                    result.getString("TYPENAME"),
                    result.getString("fullTypename") ?: "",
                    result.getString("LEVEL").toInt(),
                    result.getString("OBJECTGUID"),
                    region!!,
                    result.getString("RegionID"),
                    result.getString("Municipalitet"),
                    result.getString("MunicipalitetType"),
                    result.getString("MunicipalitetID")
                )
            }
        }

        ObjectOutputStream(FileOutputStream("streets.bin")).use {
            it.writeObject(streets)
        }

        ObjectOutputStream(FileOutputStream("allSettlements.bin")).use {
            it.writeObject(allSettlements)
        }

        ObjectOutputStream(FileOutputStream("settlements.bin")).use {
            it.writeObject(settlementsByRegFias)
        }
    }

    override fun parse() {
        super.parse()
        client.close()
    }

    override fun preRun() {
        super.preRun()
        FileWriter("${parserDir}$datasetCsvFileName").use { writer ->
            writer.write("address\n")
            parsedAddresses = Array(addresses.size) { i ->
                ownParse(addresses[i].getString("address"))
            }
            for (address in addresses) {
                writer.write(
                    address.getString("address")
                        .replace("\"", "")
//                        .replace("\\", " ")
                )
                writer.write("\n")
//                println("elastic $address")
            }
        }
//        println("elastic finished preRun")
//        exitProcess(0)
    }

    override fun readResults(stdOut: String): List<JSONObject> {
//        println("elastic result $stdOut")
        val out = stdOut.lines().findLast { it.isNotBlank() }
        val result = try {
            JSONArray(out)
        } catch (e: Exception) {
            System.err.println("elastic ${File(datasetPath).name} readResults error, trying JSON\n$out")
            e.printStackTrace()
            JSONArray()
        }

        if (result.length() == 0 && addresses.size != 0) {
            while (result.length() != addresses.size) {
                result.put(JSONObject())
            }
        }
        //
        /*
        if (addresses.size != result.length())
            while (addresses.size != result.length()) result.put(JSONObject())
*/
        /*
        var nResult = mutableListOf<JSONObject>()
        if (result.length() != addresses.size) {
            for ((i, res) in result.withIndex()) {
                var counter = 0
                var res = res.toString()
                val srcAddr = addresses[i].getString("address")
                val allowedChars = setOf<Char>(' ', '-', '_', '.', '(', ')')
                val splittedAddress = srcAddr.filter { it.isLetterOrDigit() || allowedChars.contains(it) }
                    .trim()
                    .replace("\\s+".toRegex(), " ")
                    .split(" ")
                var addrSize = splittedAddress.size

                splittedAddress.forEach {
                    val itSize = it.length
                    if (res.contains(it)) counter++
                    else {
                        var t = it
                        while (t.length > itSize / 2) {
                            println("in loop len: ${t.length}")
                            if (res.contains(t)) {
                                counter++
                                break
                            }
                            t = t.dropLast(1)
                        }
                    }
                }

                if (counter.toDouble() / addrSize < 0.85) nResult.add(JSONObject())
                else nResult.add(JSONObject(res))


            }
            println("nResult: $nResult")
            return nResult
        }
        */

//        println("elastic result $result")
        return List(result.length()) { i ->
            result.getJSONObject(i)
        }
    }

    override fun copyFromResult(srcAddress: JSONObject, result: JSONObject, index: Int) {
        val ownParsedAddress = parsedAddresses[index]

//        println("elastic copyFromResult ownParsedAddress $ownParsedAddress")
        if (!result.isEmpty) {
            result.put("normalized", getFullAddress(result))

            if (result.getString("address").isNotBlank() &&
                ((ownParsedAddress.region != null && !result.optString("region").contains(
                    ownParsedAddress.region!!.name
                )) ||
                        (ownParsedAddress.settlement.isNotBlank() && result.optString("city") != ownParsedAddress.settlement &&
                                result.optString("town") != ownParsedAddress.settlement))
            ) {
//            println("origin ${result["origin"]}")
//                        println("source ${addresses[i]}")
//            println("normalized: ${result["normalized"]}")
//            println("region ${ownParsedAddress.region?.name ?: ""} settlement ${ownParsedAddress.settlement}")
//                        println()

//                        var origin = result.getString("address")
                var origin = result.getString("origin")
                    .replace("/", "//")
                    .replace(" -", "-")
                    .replace("- ", "-")
                    .replace(" )", ")")
                    .replace("( ", "(")
                    .replace('.', ' ')
                    .replace(',', ' ')
                val requestParts = mutableListOf<String>()
                if (ownParsedAddress.region != null) {
                    requestParts += "region:\"${ownParsedAddress.region!!.fiasName}\" "
//                            request += " "
                    origin = filterFromKeywords(arrayOf(ownParsedAddress.region!!.nameLC), origin)
                    origin = filterFromRegion(origin)
                }
                if (ownParsedAddress.settlement.isNotBlank()) {
                    requestParts += "(city:\"${ownParsedAddress.settlement}\" OR town:\"${ownParsedAddress.settlement}\")"
//                            origin = filterFromKeywords(arrayOf(address.settlement.toLowerCase()), origin)
//                            origin = filterFromSettl(origin)
                }
                var foundHouse = false
                for (field in FIELDS_NAMED_FOR_FULL_ADDR) {
                    val value = result.optString(field, null) ?: continue
                    value.toIntOrNull() ?: continue
                    origin = origin.replace(value, "")
                    foundHouse = true
                }
                if (foundHouse)
                    origin = filterFromHouse(origin)
                requestParts += "fullname:($origin)"
                val request = requestParts.joinToString(" AND ")
//            println("request $request")
                searchRequest.source(
                    SearchSourceBuilder().query(
                        QueryBuilders.queryStringQuery(request)
                            .fuzziness(Fuzziness.AUTO)
                            .fuzzyMaxExpansions(3)
                    )
                )

                try {
                    val searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT)
                    if (searchResponse.hits.hits.isNotEmpty()) {
                        with(result) {
                            remove("комментарий")
                            remove("area")
                            remove("area_type")
                            remove("street")
                            remove("street_type")
                            remove("planning")
                            remove("planning_type")
                            remove("district")
                            remove("district_type")
                            remove("town_type")
                            remove("town")
                            remove("city")
                            remove("city_type")
                            remove("region")
                            remove("region_type")
                            remove("guid")
                            remove("houseguid")
                            remove("aolevel")
                        }
//                            result= JSONObject(searchResponse.hits.hits[0].sourceAsMap)
                        result.toMap().apply {
                            putAll(searchResponse.hits.hits[0].sourceAsMap)
                            result.clear()
                            for ((key, value) in this) {
                                result.put(key, value)
                            }
                        }
                    }
                } catch (e: ElasticsearchException) {
                    System.err.println("ElasticsearchException request:\n$request\nmessage:\n${e.detailedMessage}")
                }

                result.put("normalized", getFullAddress(result))
//            println("corrected: ${result["normalized"]}")
//            println()
            }
            getErrorFilteringRequest(result)?.let {
                result.put("normalized", getFullAddress(result))
                result.put("filtered", 1)
//            println("filtered: ${result["normalized"]}")
//            println()
            }

            if (!srcAddress.has("city")) {
                val cityParts = mutableListOf<String>()
                result.opt("city")?.apply {
                    with(toString()) {
                        if (isNotBlank()) cityParts += this
                    }
                }
                result.opt("town")?.apply {
                    with(toString()) {
                        if (isNotBlank()) cityParts += this
                    }
                }
                result.opt("city_type")?.apply {
                    with(toString()) {
                        if (isNotBlank() && cityParts.size == 2) cityParts.add(0, this)
                    }
                }
                result.opt("town_type")?.apply {
                    with(toString()) {
                        if (isNotBlank() && cityParts.size >= 2) cityParts.add(2, this)
                    }
                }

                srcAddress.put("city", cityParts.joinToString(" "))
            }

            if (!srcAddress.has("house")) {
                val house = StringBuilder().append(result.optString("housenum"))
                result.opt("корпус")?.let {
                    house.append(" корпус ").append(it)
                }
                result.opt("строение")?.let {
                    house.append(" строение ").append(it)
                }
                srcAddress.put("house", house.toString().trim())
            }

            if (!srcAddress.has("unit")) {
                val unit = StringBuilder()
                result.opt("помещение")?.let {
                    unit.append(" помещение ").append(it)
                }
                result.opt("квартира")?.let {
                    unit.append(" квартира ").append(it)
                }
                result.opt("офис")?.let {
                    unit.append(" офис ").append(it)
                }
                srcAddress.put("unit", unit.toString().trim())
            }
        }
    }

    override val FIELDS_TRANSLATION = mapOf(
        "postalcode" to arrayOf("postalcode"),
        "region_type" to arrayOf("region_type"),
        "region" to arrayOf("region"),
        "city_type" to arrayOf("city_type"),
        "city" to arrayOf("city"),
        "district" to arrayOf("district"),
        "district_type" to arrayOf("district_type"),
        "street_type" to arrayOf("street_type"),
        "street" to arrayOf("street"),
        "unit" to arrayOf("rm"),
        "unit_type" to arrayOf("rm_type")
    )

    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(datasetCsvFileName).delete()
        }
    }


    companion object {


        fun filterFromRegion(address: String): String {
            return filterFromKeywords(REGION_KEYWORDS, address)
        }

        fun filterFromSettl(address: String): String {
            return filterFromKeywords(SETTL_KEYWORDS, address)
        }

        fun filterFromHouse(address: String): String {
            return filterFromKeywords(HOUSE_KEYWORDS, address)
        }

        private fun filterFromKeywords(keywords: Array<String>, address: String): String {
            val tokens = address.lowercase(Locale.getDefault()).split(" ", ".", ",").toMutableList()
            for (keyword in keywords) {
                var i = 0
                while (i < tokens.size) {
                    if (tokens[i] == keyword)
                        tokens.removeAt(i)
                    else
                        i++
                }
            }

            return tokens.joinToString(" ")
        }

        fun getFullAddress(data: JSONObject): String {
            val fullAddress = StringBuilder()
            for (key in FIELDS_FOR_FULL_ADDR) {
                data.opt(key)?.let { value ->
                    fullAddress.append(value).append(" ")
                }
            }
            for (key in FIELDS_NAMED_FOR_FULL_ADDR) {
                data.opt(key)?.let { value ->
                    fullAddress.append(key).append(" ").append(value).append(" ")
                }
            }
            return fullAddress.toString()
        }

        fun containsAtLeastOne(where: String, what: String): Boolean {
            if (what.isBlank() || what.length <= 2) return false
            val words = what.split(" ", "-", ",", "/", ";")
            for (word in words) {
                if (word.toIntOrNull() != null) continue
                if (word.length <= 2) continue
                if (where.contains(word)) return true
            }

            return false
        }

        fun containsAll(where: String, what: String): Boolean {
            if (what.isBlank() || what.length <= 2) return false
//            val words = what.split(" ", "-", ",", "/", ";")
            val words = what.split(" ")
            var result = true
            for (word in words) {
                if (word.toIntOrNull() != null) continue
                if (word.length <= 2) continue
                result = result && where.contains(word)
                if (!result) return false
            }

            return true
        }

        fun getErrorFilteringRequest(data: JSONObject): String? {
            var address = data.getString("origin").lowercase(Locale.getDefault())
            do {
                val len = address.length
                address = address.replace("  ", "")
            } while (len > address.length)
            address = address
                .replace(" -", "-")
                .replace("- ", "-")
                .replace(" )", ")")
                .replace("( ", "(")
//                .replace(" ", "")
            val street = data.optString("street").lowercase(Locale.getDefault())
//                .replace(" ", "")
//                .replace("-", "")
            val planning = data.optString("planning").lowercase(Locale.getDefault())
//                .replace(" ", "")
//                .replace("-", "")
            val filteredStreet = street.isNotBlank() && !containsAll(address, street) ||
                    planning.isNotBlank() && !containsAll(address, planning)


            val city = data.optString("city").lowercase(Locale.getDefault())
//                .replace(" ", "")
//                .replace("-", "")
            val town = data.optString("town").lowercase(Locale.getDefault())
//                .replace(" ", "")
//                .replace("-", "")
            val filteredCity = city.isNotBlank() && !containsAll(address, city)
            val filteredTown = town.isNotBlank() && !containsAll(address, town)

            val request = StringBuilder()

            if (filteredStreet || filteredCity || filteredTown) {
//                println("\n")
//                println("origin: ${data.getString("origin")}")
//                println("address: $address")
//                println("parsed: ${getFullAddress(data)}")
                data.remove("комментарий")

                if (filteredStreet) {
//                    println("filtered street $street & planning $planning")
                    data.remove("street")
                    data.remove("street_type")
                    data.remove("planning")
                    data.remove("planning_type")
                    data.remove("district")
                    data.remove("district_type")

                    if (street.isNotBlank())
                        request.append(" -street:\"$street\"")
                    if (planning.isNotBlank())
                        request.append(" -planning:\"$planning\"")
                }

                if (filteredCity || filteredTown) {
                    if (filteredTown) {
//                        println("filtered town $town")
                        data.remove("town_type")
                        data.remove("town")
                        request.append(" -town:\"$town\"")
                    }
                    if (filteredCity) {
//                        println("filtered city $city")
                        data.remove("city")
                        data.remove("city_type")
                        request.append(" -city:\"$city\"")
                    }
                    data.remove("region")
                    data.remove("region_type")
                    data.remove("street")
                    data.remove("street_type")
                    data.remove("planning")
                    data.remove("planning_type")
                    data.remove("district")
                    data.remove("district_type")
                }
                data.optString("guid", null)?.let { guid ->
                    request.append(" -guid:\"$guid\"")
                }
                data.remove("guid")
                data.remove("houseguid")
                data.remove("aolevel")

//                if (city.isNotBlank() && !filteredCity)
//                    request.append(" city:\"$city\"")
//                if (town.isNotBlank() && !filteredTown)
//                    request.append(" town:\"$town\"")
//                if (street.isNotBlank() && !filteredStreet)
//                    request.append(" street:\"$street\"")
//                if (planning.isNotBlank() && !filteredStreet)
//                    request.append(" planning:\"$planning\"")
            }

            return if (request.isNotBlank()) request.toString() else null
        }

        val NAME_CORRECTION = mapOf(
            "Саха /Якутия/" to "Якутия",
            "Северная Осетия - Алания" to "Северная Осетия",
            "Ханты-Мансийский Автономный округ - Югра" to "Ханты-Мансийский",
            "Кемеровская область - Кузбасс" to "Кемеровская"
        )

        val REGION_KEYWORDS = arrayOf(
            "обл",
            "область",
            "ао",
            "респ",
            "республика",
            "округ",
            "автономный",
            "край",
            "кр"
        )
        val HOUSE_KEYWORDS = arrayOf(
            "кв",
            "квартира",
            "дом",
            "д",
            "оф",
            "офис",
            "корпус",
            "корп",
            "к",
            "лит",
            "литера",
            "литер",
            "стр",
            "ст",
            "строение",
            "двлд",
            "ком",
            "комната",
            "пом",
            "помещение"
        )
        val SETTL_KEYWORDS = arrayOf(
            "город",
            "г",
            "поселок",
            "посёлок",
            "пос",
            "село",
            "с",
            "пгт",
            "городского",
            "типа",
            "станция",
            "станица"

        )
        val FIELDS_NAMED_FOR_FULL_ADDR = arrayOf(
            "дом",
            "корпус",
            "литера",
            "строение",
//            "Корпус/строение",
            "квартира",
            "комната",
            "офис",
            "число",
            "помещение",
            "прочее"
        )
        val FIELDS_FOR_FULL_ADDR = arrayOf(
            "region_type",
            "region",
            "area_type",
            "area",
            "city_type",
            "city",
            "town_type",
            "town",
            "district_type",
            "district",
            "street_type",
            "street",
            "planning_type",
            "planning"
        )
        val FIELDS_TO_REMOVE = arrayOf(
            "origin",
            "normalized",
            "region_type",
            "region",
            "area_type",
            "area",
            "city_type",
            "city",
            "town_type",
            "town",
            "district_type",
            "district",
            "street_type",
            "street",
//            "fullname",
            "planning_type",
            "planning",
            "house",
            "housenum",
            "strucnum",
            "buildnum",
            "additional_type",
            "additional",
            "postalcode",

//            "Дом",
            "дом",
//            "корпус",
            "литера",
            "строение",
            "Корпус/строение",
            "квартира",
            "комната",
            "офис",
            "число",
            "помещение",
            "прочее",
            "комментарий",

            "guid",
            "houseguid",
            "aolevel"
        )
        val FIELDS = arrayOf(
            "origin",
            "normalized",
            "filtered",
            "region_type",
            "region",
            "area_type",
            "area",
            "city_type",
            "city",
            "town_type",
            "town",
            "district_type",
            "district",
            "street_type",
            "street",
//            "fullname",
            "planning_type",
            "planning",
            "house",
            "housenum",
            "strucnum",
            "buildnum",
            "additional_type",
            "additional",
            "postalcode",

//            "Дом",
            "дом",
//            "корпус",
            "литера",
            "строение",
            "Корпус/строение",
            "квартира",
            "комната",
            "офис",
            "число",
            "помещение",
            "прочее",
            "комментарий",

            "guid",
            "houseguid",
            "aolevel"
        )
    }

    class Address(
        var region: Region? = null,
        var settlement: String = "",
        var street: String = ""
    ) : Serializable {
        override fun toString(): String = "${region ?: ""} $settlement $street"
    }

    class Region(
        val name: String,
        val otherName: String,
        val fiasName: String,
        val typeShort: String,
        val typesLong: Array<String>,
        val fias: String,
        val nameSufficient: Boolean = false
    ) : Serializable {
        val variants: List<String>
        val nameLC: String = name.lowercase(Locale.getDefault())

        init {
            val variants = mutableListOf<String>()
            this.variants = variants
            val add = { name: String ->
                for (type in typesLong) {
                    variants += "$name $type"
                    variants += "$type $name"
                }
                variants += "$typeShort $name"
                variants += "$name $typeShort"
            }
            if (nameSufficient) {
                variants += nameLC
                if (otherName.isNotBlank()) variants += otherName.lowercase(Locale.getDefault())
            } else {
                add(nameLC)
                if (otherName.isNotBlank()) add(otherName.lowercase(Locale.getDefault()))
            }
        }

        override fun toString(): String {
            return fiasName
        }
    }

    class SettlementCluster(
        val name: String,
        val settlements: MutableList<Settlement> = mutableListOf()
    ) : Serializable {
        val nameLC = name.lowercase(Locale.getDefault())
        override fun toString(): String {
            return "$name (${settlements.joinToString(", ") { it.region }})"
        }

    }

    class Settlement(
        val name: String,
        val typeShort: String,
        val typeFull: String,
        val level: Int,
        val fiasID: String,
        val region: String,
        val regionFias: String?,
        val municipality: String?,
        val municipalityType: String?,
        val municipalityID: String?
    ) : Serializable {
        val nameLC = name.lowercase(Locale.getDefault())

        override fun toString(): String {
            return "$typeFull $name ($region)"
        }
    }

    class Street(val name: String, val type: String, val level: Int = 0, val territory: String? = null) : Serializable {
        override fun toString(): String {
            return "$type $name " + (territory?.run { "($this)" } ?: "")
        }
    }
}