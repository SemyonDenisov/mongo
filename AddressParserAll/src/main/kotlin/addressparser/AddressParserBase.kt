package ru.samis.addressparser

import org.json.JSONException
import org.json.JSONObject
import java.io.*
import java.nio.charset.Charset
import java.util.*

abstract class AddressParserBase(
    val parserDir: String,
    var datasetPath: String,
    var outFile: String
) {
    protected val addresses = mutableListOf<JSONObject>()
    protected abstract val externalCommand: String
    protected lateinit var builder: ProcessBuilder
    private var iBasket = 0
    protected var totalCount = 0
    protected lateinit var dic: JSONObject
    private val FIELDS: MutableMap<String, Array<String>> = mutableMapOf()
    private var initialized = false

    //
    val options by lazy {
        JSONObject(File("settings.json").readText()).getJSONObject("options")
    }

    open fun init() {
        FIELDS.putAll(EMPTY_FIELDS_TRANSLATION)
        FIELDS.putAll(FIELDS_TRANSLATION)

        loadDic()

        initialized = true
    }

    open fun parse() {

        if (!initialized) init()
        //println("builder: ${builder.command()}")
        //builder = ProcessBuilder(listOf("sh", "-c", externalCommand)).directory(File(parserDir))

        totalCount = 0
        processByPortions()
    }

    open fun loadDic() {
        dic = JSONObject(File("dic.json").readText())
    }

    open fun preRun() {
        if (!initialized) init()
    }

    open fun copyFromResult(srcAddress: JSONObject, result: JSONObject, index: Int) {}

    open fun preProcessInputAddress(address: String) = address

    protected fun processByPortions() {
        var time = -System.nanoTime()
        iBasket = 0
        OutputStreamWriter(FileOutputStream(outFile), "windows-1251").use { writer ->
            File(datasetPath).reader(Charset.forName("windows-1251")).forEachLine { line ->
                addresses += try {
                    JSONObject(line/*.replace("\\", "\\\\")*/).apply {
                        put("address", preProcessInputAddress(optString("address")))
                    }
                } catch (e: Exception) {
                    JSONObject().put("address", preProcessInputAddress(line))
                }

                totalCount++

                if (addresses.size == PORTION_SIZE) {
                    processBasket(writer)

                    addresses.clear()

                    println()
                }
            }

            if (addresses.isNotEmpty()) {
                processBasket(writer)
                addresses.clear()
            }
        }

        time += System.nanoTime()
        println("${javaClass.simpleName} ${File(datasetPath).name} time ${time / 1e6}")
    }

    open fun readResults(stdOut: String): List<JSONObject> {
        val result = mutableListOf<JSONObject>()
        for (line in stdOut.lines()) {
            if (line.isBlank()) continue
            result += try {
                JSONObject(line)
            } catch (e: JSONException) {
                println("${javaClass.simpleName} ${File(datasetPath).name} JSONException in line $line")
                JSONObject()
            }
        }
        if (result.size == 0 && addresses.size != 0) {
            while (result.size != addresses.size) {
                result.add(JSONObject())
            }
        }
        /*
        if (addresses.size != result.size)
            while (addresses.size != result.size) result.add(JSONObject())
        */
        return result
    }

    private fun processBasket(writer: OutputStreamWriter) {
        println("${javaClass.simpleName} ${File(datasetPath).name} processing basket ${iBasket++}")
        preRun()

        val runtimeProcess = builder.start()

        //runtimeProcess.waitFor()
        //println("command finished ${builder.command()}")
        val results = readResults(runtimeProcess.inputStream.bufferedReader().readText())
        println("results: $results")

        cleanTempFiles()
        println("src size: ${addresses.size}")
        println("result size: ${results.size}")

        for ((iAddr, result) in results.withIndex()) {
            val srcAddress = addresses[iAddr]

            copyFromResult(srcAddress, result, iAddr)

            for ((destName, srcNames) in FIELDS) {
                if (srcAddress.has(destName)) continue
                srcAddress.put(
                    destName,
                    srcNames.map { result.optString(it) }.filter { it.isNotBlank() }.joinToString(" ")
                )
            }
            postProcess(srcAddress)
            writeResultLine(writer, srcAddress)
        }
    }

    open fun postProcess(result: JSONObject) {
//        println(result)
        val newValues = mutableMapOf<String, String>()
        for (addressKey in result.keys()) {
            val shorts = dic.optJSONObject(addressKey) ?: continue

            var value = result.getString(addressKey).trim()
            do {
                val length = value.length
                value = value.replace("  ", " ")
            } while (length > value.length)
            value = value.replace(" ,", ",").replace(" .", ".")
            newValues[addressKey] = value

            val filteredValue = value.lowercase(Locale.getDefault()).replace('.', ' ')
            var filteredType =
                result.optString("${addressKey}_type").trim().lowercase(Locale.getDefault()).replace('.', ' ')
//            println(filteredType)

            var foundCount = 0
            var foundFullName = ""
            var foundShortName = ""
            var foundIndex = -1

            for (fullName in shorts.keys()) {
                val shortNames = shorts.getJSONArray(fullName)

                val find = { name: String ->
                    val index = findWord(filteredValue, name)
                    if (index >= 0) {
                        if (foundCount == 0) {
                            foundShortName = name
                            foundFullName = fullName
                            foundIndex = index
                        }
                        foundCount++
                    }
                }

                find(fullName.lowercase(Locale.getDefault()))

                for (i in 0 until shortNames.length()) {
                    val shortName = shortNames.getString(i).lowercase(Locale.getDefault()).trim()
                    find(shortName)
                    val index = findWord(filteredType, shortName)
                    if (index >= 0) {
                        filteredType = filteredType.replaceRange(
                            index, index + shortName.length, fullName.lowercase(Locale.getDefault())
                        )
//                        println("replaced ${newValues["${addressKey}_type"]} with $filteredType")
                        newValues["${addressKey}_type"] = filteredType
//                        println(newValues)
                    }
                }
            }

            if (foundCount == 1) {
                val index = if (foundIndex > 0 && value[foundIndex - 1] == ' ') foundIndex - 1 else foundIndex
                val length =
                    if (index == foundIndex - 1 || (index + foundShortName.length < value.length && DELIMITERS_AFTER.contains(
                            value[foundIndex + foundShortName.length]
                        ))
                    )
                        foundShortName.length + 1
                    else
                        foundShortName.length
                value = value.replaceRange(index, index + length, "").trim()
//                println("found one")
                newValues[addressKey] = value
                newValues["${addressKey}_type"] = foundFullName.lowercase(Locale.getDefault())
//                println(newValues)
            }
        }

//        println(newValues)
        for ((key, value) in newValues) {
            result.put(key, value)
        }

        for (addressKey in result.keys()) {
            val value = result.optString(addressKey, null) ?: continue
            val len = value.length
            val words = value.split(" ")
            when (addressKey) {
                "region" -> {
                    if (len < 4 || words.size > 2) result.put(addressKey, "")
                }

                "city" -> {
                    if (len < 3 || words.size > 2) result.put(addressKey, "")
                    val letterDigit = words.any {
                        var num = -1
                        var letters = false
                        for (i in it.length - 1 downTo 0) {
                            when {
                                it[i].isDigit() -> num = it[i].toString().toInt()
                                it[i] == '-' -> {
                                    num = -1
                                }

                                else -> letters = num >= 0
                            }
                        }
                        letters
                    }
                    if (letterDigit) result.put(addressKey, "")
                }

                "street" -> {
                    if (words.size > 3) result.put(addressKey, "")
                }

                "house" -> {
                    val filtered = words.filterNot { HOUSE_KEYWORDS.contains(it.lowercase(Locale.getDefault())) }
                    if (filtered.size > 3) result.put(addressKey, "")
                }
            }
        }
//        println(result)
    }

    val HOUSE_KEYWORDS =
        hashSetOf("лит", "корп", "к", "литер", "литера", "корпус", "стр", "строение", "с", "ст", "дом", "д")

    open fun writeResultLine(writer: OutputStreamWriter, result: JSONObject) {
        writer.write(result.toString())
        writer.write("\n")
        writer.flush()
    }

    abstract val FIELDS_TRANSLATION: Map<String, Array<String>>

    abstract fun cleanTempFiles()



    companion object {
        //
        val options by lazy {
            JSONObject(File("settings.json").readText()).getJSONObject("options")
        }
        val PORTION_SIZE = options.optInt("PORTION_SIZE", 1000000)


        val DELIMITERS_AFTER = hashSetOf('.', ' ')

        fun findWord(phrase: String, word: String, startIndex: Int = 0): Int {
            if (word.toIntOrNull() != null) return -1
            var result = -1
            var index = startIndex - 1
            do {
                val oldIndex = index
                index = phrase.indexOf(word, index + 1, true)
                if (index < 0) break
                if (index > 0 && (phrase[index - 1].isLetterOrDigit() ||
                            phrase[index - 1] == '-')
                ) continue
                if (index + word.length < phrase.length && (
                            phrase[index + word.length].isLetterOrDigit() ||
                                    phrase[index + word.length] == '-')
                ) continue
                result = index
            } while (index > oldIndex && result < 0)

            return result
        }

        val EMPTY_FIELDS_TRANSLATION = mapOf(
            "not_decomposed" to arrayOf("not_decomposed"),
            "postalcode" to arrayOf("postalcode"),
            "region_type" to arrayOf("region_type"),
            "region" to arrayOf("region"),
            "district_type" to arrayOf("district_type"),
            "district" to arrayOf("district"),
            "city_type" to arrayOf("city_type"),
            "city" to arrayOf("city"),
            "street_type" to arrayOf("street_type"),
            "street" to arrayOf("street"),
            "house_type" to arrayOf("house_type"),
            "house" to arrayOf("house"),
            "unit" to arrayOf("unit"),
            "unit_type" to arrayOf("unit_type")
        )
    }
}
