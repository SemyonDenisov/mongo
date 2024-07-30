package ru.samis.addressparser

import org.json.JSONObject
import java.io.File
import java.io.LineNumberReader
import java.nio.charset.Charset
import java.util.*
import kotlin.concurrent.thread
import kotlin.math.min

public class UnitedParser {
    private var statuses = Array(0) { 0 }
    private var parsers = arrayOf<AddressParserBase>()
    private val options by lazy {
        JSONObject(File("settings.json").readText()).getJSONObject("options")
    }
    private lateinit var filesKeys: Array<String>
    private lateinit var outFiles: List<String>
    private var initialized = false

    private fun mergeResults(outFiles: List<String>, outUnitedFile: String) {
        val bestIndexes = mutableMapOf<String, List<Int>>()
        val elseIndexes = mutableMapOf<String, List<Int>>()
        val allIndexes = mutableMapOf<String, MutableList<Int>>()
        val fieldsBestIndexes = options.getJSONObject("fieldsBest")
        val fieldsElseIndexes = options.getJSONObject("fieldsElse")
        for (key in fieldsBestIndexes.keys()) {
            val bestKeyIndexes = fieldsBestIndexes.getJSONArray(key)

            bestIndexes += key to List(bestKeyIndexes.length()) { i ->
                bestKeyIndexes.getInt(i)
            }
            allIndexes += key to MutableList(bestKeyIndexes.length()) { i ->
                bestKeyIndexes.getInt(i)
            }
        }
        for (key in fieldsElseIndexes.keys()) {
            val elseKeyIndexes = fieldsElseIndexes.getJSONArray(key)

            val allKeyIndexes = allIndexes.getOrPut(key) { mutableListOf() }
            elseIndexes += key to List(elseKeyIndexes.length()) { i ->
                elseKeyIndexes.getInt(i).apply { allKeyIndexes.add(this) }
            }
        }

        val readers = outFiles.map { LineNumberReader(File(it).reader(Charset.forName("windows-1251"))) }

        val specialWords = (options.optJSONObject("specialWords") ?: JSONObject()).toMap().mapValues {
            it.value as Map<String, Int>
        }
        val algorithms = options.optJSONObject("algorithmsForCommonValue") ?: JSONObject()

        File(outUnitedFile)
            .writer(Charset.forName("windows-1251")).use { writer ->
                var lines = readers.map { it.readLine() }

                while (lines.all { it != null }) {
                    val jsons = lines.map { JSONObject(it) }
                    val result = JSONObject(jsons[0].toMap())
//                    println(jsons[0]["address"])

                    for (key in KEYS) {
                        val keyIndexes = bestIndexes[key]!!
                        val commonValue = when (algorithms.optString(key)) {
                            "firstAlgorithm" ->
                                firstAlgorithm(key, keyIndexes, elseIndexes, jsons)

                            "secondAlgorithm" -> secondAlgorithm(
                                key,
                                keyIndexes,
                                elseIndexes,
                                specialWords[key]!!,
                                result, jsons
                            )

                            else ->
                                selectOld(key, allIndexes, keyIndexes, elseIndexes, jsons)
                        }
                        result.put(key, commonValue)
                    }

                    writer.write(result.toString())
                    writer.write("\n")

                    lines = readers.map { it.readLine() }
                }
            }

        readers.forEach { it.close() }
    }

    fun selectOld(
        key: String,
        allIndexes: Map<String, List<Int>>,
        keyIndexes: List<Int>,
        elseIndexes: Map<String, List<Int>>,
        jsons: List<JSONObject>
    ): String {
        if (allIndexes[key]!!.size >= 2 && key != "house" && key != "unit") {
            val fieldValues = Array(5) { index ->
                jsons[index].optString(key).replace(".", " ").replace(",", " ").trim().lowercase(Locale.getDefault())
            }
//                        if (fieldValues.count { it.isNotBlank() } < 2) {
//                            result.put(key, "")
//                            continue
//                        }
            val allWords = mutableListOf<String>()
            for (fieldValue in fieldValues) {
                val words = fieldValue.split(" ")
                allWords.addAll(words.filterNot {
                    if (it.isBlank()) return@filterNot true
                    var count = 0
                    for (c in it) {
                        if (c.isLetter()) count++
                    }
                    count < 3
                })
            }
            val oftenWords = mutableListOf<String>()
            for (word in allWords) {
                val freq = fieldValues.count { AddressParserBase.findWord(it, word) >= 0 }
                if (freq >= 2) oftenWords.add(word)
            }
            if (oftenWords.isEmpty()) {
                return ""
            }
        }

        var value = select(jsons, key, allIndexes[key]!!, keyIndexes.size)
        if (value.isBlank()) value =
            select(jsons, key, elseIndexes[key] ?: listOf(), elseIndexes[key]?.size ?: 0)
        return value
    }


    private fun select(jsons: List<JSONObject>, key: String, keyIndexes: List<Int>, count: Int): String {
        val fieldValues = mutableMapOf<Int, String>()
        val notBlankIndexes = mutableListOf<Int>()
        var i = 0
        while (fieldValues.size < count && i < keyIndexes.size) {
            val index = keyIndexes[i]
            with(jsons[index].optString(key)) {
                if (isNotBlank()) {
                    fieldValues[index] = this
                    notBlankIndexes += index
                }
            }
            i++
        }


        if (fieldValues.isEmpty()) return ""
        if (fieldValues.size <= 2) return fieldValues[notBlankIndexes[0]]!!

        val minDists = notBlankIndexes.map { index ->
            //для каждого разборщика составляем минимальные дистанции
            // левенштейна до результатов остальных
            val srcValue =
                prepareToCompare(fieldValues[index]!!) //значение поля для очередного разборщика
            val dists = notBlankIndexes.map { index2 ->
                //считаем дистанции до всех, кроме себя
                if (index == index2) Int.MAX_VALUE - 1 else {
                    val targetValue = prepareToCompare(fieldValues[index2]!!)
                    if (srcValue.isBlank() || targetValue.isBlank())
                        Int.MAX_VALUE
                    else
                        Companion.levenshtein(srcValue, targetValue)
                }
            }
            dists.min()
        }

        val indexOfMin = minDists.indexOf(minDists.min())
        return jsons[notBlankIndexes[indexOfMin]].getString(key)!!
    }

    fun init(datasetFile: String) {

        val runYandex = options.optBoolean("runYandex", false)

        val runNatasha = options.optBoolean("runNatasha", true)
        val runLibpostal = options.optBoolean("runLibpostal", true)
        val runCuda = options.optBoolean("runCuda", true)
        val runDeepparse = options.optBoolean("runDeepparse", true)
        val runElastic = options.optBoolean("runElastic", true)

        this.filesKeys = FILES_KEYS.copyOfRange(0, if (runYandex) FILES_KEYS.size else FILES_KEYS.size - 1)
        val filesKeys = FILES_KEYS.filterIndexed { index, _ ->
            when (index) {
                0 -> runNatasha
                1 -> runLibpostal
                2 -> runCuda
                3 -> runDeepparse
                4 -> runElastic
                5 -> runYandex
                else -> true
            }
        }
        println("val filesKeys: $filesKeys")
        println("filesKeys: ${this.filesKeys.toList()}")

        val outFiles = filesKeys.mapIndexed { i, key ->
            datasetFile + "_" + options.optString(key, OUT_FILES[i])
        }
        println("val outKeys: $outFiles")


        //
        statuses = Array(this.filesKeys.size) { 0 }

       // val dirs = DIRS_KEYS.map { options.getString(it) }

        val dirs = DIRS_KEYS.mapIndexedNotNull { index, key ->
            when (key) {
                "geonormCatalog" -> if (runNatasha) options.getString(key) else null
                "libpostalDir" -> if (runLibpostal) options.getString(key) else null
                "cudaParserDir" -> if (runCuda) options.getString(key) else null
                "deepparseDir" -> if (runDeepparse) options.getString(key) else null
                "elasticParserDir" -> if (runElastic) options.getString(key) else null
                "yandexDir" -> if (runYandex) options.getString(key) else null
                else -> null
            }
        }.toTypedArray()

        parsers = filesKeys.mapIndexedNotNull { i, key ->
            when (key) {
                "natashaOutFile" -> if (runNatasha) AddressParserNatasha(dirs[i], datasetFile, outFiles[i]) else null
                "libpostalOutFile" -> if (runLibpostal) AddressParserLibpostal(dirs[i], datasetFile, outFiles[i]) else null
                "cudaOutFile" -> if (runCuda) AddressParserCuda(dirs[i], datasetFile, outFiles[i]) else null
                "deepparseOutFile" -> if (runDeepparse) AddressParserDeepparse(dirs[i], datasetFile, outFiles[i]) else null
                "elasticOutFile" -> if (runElastic) AddressParserElastic(
                    datasetFile, dirs[i], outFiles[i],
                    options.getString("elasticHost"),
                    options.getInt("elasticPort"),
                    options.getString("elasticScheme")
                ) else null
                "yandexOutFile" -> if (runYandex) AddressParserYandex(dirs[i], datasetFile, outFiles[i]) else null
                else -> null
            }
        }.toTypedArray()

        parsers.forEach {
            println(it)
            println(it.parserDir)
            println(it.datasetPath)
            println(it.outFile)
            println()
            it.init()
        }

        initialized = true
        /*

      //  val runYandex = options.optBoolean("runYandex", false)

        filesKeys = FILES_KEYS.copyOfRange(0, if (runYandex) FILES_KEYS.size else FILES_KEYS.size - 1)
        println("filesKeys: ${filesKeys.toList()}")

        outFiles = filesKeys.mapIndexed { i, key ->
            datasetFile + "_" + options.optString(key, OUT_FILES[i])
        }
        println("outFiles: $outFiles")

        statuses = Array(filesKeys.size) { 0 }

        val dirs = DIRS_KEYS.map { options.getString(it) }

        parsers = Array(filesKeys.size) { i ->
            when (i) {
                0 -> AddressParserNatasha(dirs[i], datasetFile, outFiles[i])

                1 -> AddressParserLibpostal(dirs[i], datasetFile, outFiles[i])

                2 -> AddressParserCuda(dirs[i], datasetFile, outFiles[i])

                3 -> AddressParserDeepparse(dirs[i], datasetFile, outFiles[i])

                4 -> AddressParserElastic(
                    datasetFile, dirs[i], outFiles[i],
                    options.getString("elasticHost"),
                    options.getInt("elasticPort"),
                    options.getString("elasticScheme")
                )

                5 -> AddressParserYandex(dirs[i], datasetFile, outFiles[i])

                else -> AddressParserYandex(dirs[i], datasetFile, outFiles[i])

            }
        }
        parsers.forEach { it.init() }
        initialized = true

         */

    }

    fun parse(datasetFile: String, outUnitedFile: String) {
//        val runYandex = options.optBoolean("runYandex", false)
//        filesKeys = FILES_KEYS.copyOfRange(0, if (runYandex) FILES_KEYS.size else FILES_KEYS.size - 1)
//        outFiles = filesKeys.mapIndexed { i, key ->
//            datasetFile + "_" + options.optString(key, OUT_FILES[i])
//        }
//        mergeResults(outFiles, outUnitedFile)

        val runParallel = options.optBoolean("parallel", false)

        if (!initialized) init(datasetFile)

        outFiles = filesKeys.mapIndexed { i, key ->
            datasetFile + "_" + options.optString(key, OUT_FILES[i])
        }
        println("outKeys: ${this.outFiles.toList()}")

        /*
        parsers.forEachIndexed { index, it ->
            it.datasetPath = datasetFile
            it.outFile = outFiles[index]
        }

         */
        statuses = Array(filesKeys.size) { 0 }

        //

        if (runParallel) {
            parsers.indices.forEach { i -> startParserThread(i) }
            while (statuses.any { it == 0 }) {
                println("sleeping")
                synchronized(statuses) {
                    (statuses as Object).wait()
                }
            }
        } else {
            parsers.indices.forEach { i -> parsers[i].parse() }
        }
        mergeResults(outFiles, outUnitedFile)
    }

    private fun startParserThread(index: Int) {
        thread {
            var time = -System.nanoTime()
            try {
                parsers[index].parse()
                time += System.nanoTime()

                println("${parsers[index].javaClass.simpleName} finished, time ${time / 1e9}")
            } catch (e: Exception) {
                println("${parsers[index].javaClass.simpleName} exception ${e.localizedMessage}")
                e.printStackTrace()
            }

            synchronized(statuses) {
                statuses[index] = 1
                (statuses as Object).notifyAll()
            }
        }
    }

    companion object {
        private val FILES_KEYS = arrayOf(
            "natashaOutFile", "libpostalOutFile", "cudaOutFile", "deepparseOutFile", "elasticOutFile", "yandexOutFile"
        )

        private val OUT_FILES = arrayOf(
            "out_natasha.jsonl",
            "out_libpostal.jsonl",
            "out_cuda.jsonl",
            "out_deepparse.jsonl",
            "out_elastic.jsonl",
            "out_yandex.jsonl"
        )

        private val DIRS_KEYS =
            arrayOf("geonormCatalog", "libpostalDir", "cudaParserDir", "deepparseDir", "elasticParserDir", "yandexDir")

        fun secondAlgorithm(
            field: String,
            bestIndexes: List<Int>,
            elseIndexes: Map<String, List<Int>>,
            specialWords: Map<String, Int>,
            result: JSONObject,
            jsons: List<JSONObject>
        ): String {
            val values = mutableListOf<String>()
            val rawValues = mutableListOf<String>()

            val proc = { subIndex: Int ->
                val str = jsons[subIndex].getString(field)
                if (!isTooShort(str, 1)) {
                    values.add(str)
                    rawValues.add(jsons[subIndex].getString(field))
                }
            }

            bestIndexes.forEach(proc)

            if (differentValues(values) || values.size < 2) {
                elseIndexes.getOrElse(field) { emptyList() }.forEach(proc)
            }

            val commonValue = mostPolularWordsValue(values, specialWords) ?: return ""
            var rawValue = rawValues[commonValue]

            if (field == "unit") { //особый случай, обход недостатков некоторых разборщиков
                val usedWords = mutableMapOf<String, Int>()

                //собираем все слова в исходном адресе
                val address = filter2(jsons[0].getString("address"))
                var parts = address.split(" ")

                parts.forEach { part ->
                    if (part.isNotEmpty()) {
                        val count = usedWords.getOrDefault(part, 0)
                        usedWords[part] = count + 1
                    }
                }

                //убираем слова, которые уже были в других частях
                listOf("region", "city", "street", "house").forEach { lastField ->
                    val commonValue = filter2(result.optString(lastField))
                    val parts = commonValue.split(" ")

                    parts.forEach { part ->
                        if (part.isNotEmpty() && usedWords.containsKey(part)) {
                            usedWords[part] = usedWords[part]!! - 1
                        }
                    }
                }

                //оставляем в номере квартиры только слова, которые еще можно
                val existingParts = mutableListOf<String>()
                parts = rawValue.split(" ")

                parts.forEach { part ->
                    if (part.isNotEmpty()) {
                        val part = part.lowercase(Locale.getDefault())
                        val count = usedWords.getOrDefault(part, 0)
                        if (count > 0) {
                            existingParts.add(part)
                            usedWords[part] = count - 1
                        }
                    }
                }

                rawValue = existingParts.joinToString(" ")
            }

            return rawValue
        }

        fun firstAlgorithm(
            key: String,
            keyIndexes: List<Int>,
            elseIndexes: Map<String, List<Int>>,
            jsons: List<JSONObject>
        ): String? {
            val values = mutableListOf<String>()
            val rawValues = mutableListOf<String>()

            val proc = { str: String ->
                if (isValid(str, key)) {
                    val filtered = filter(str)
                    if (!isTooShort(filtered)) {
                        values += filtered
                        rawValues += str
                    }
                }
            }

            keyIndexes.map { index -> proc(jsons[index].getString(key)) }

            if (differentValues(values) || values.size < 2) {
                elseIndexes.getOrElse(key) { emptyList() }.map { index -> proc(jsons[index].getString(key)) }
            }

            return medianValue(values)?.let { rawValues[it] }
        }

        private fun mostPolularWordsValue(values: List<String>, specialWords: Map<String, Int>): Int? {
            if (values.isEmpty()) {
                return null
            }
            if (values.size == 1) {
                return 0
            }

            val words = mutableMapOf<String, Int>()
            for (value in values) {
                val filteredValue = filter2(value)
                val parts = filteredValue.split(" ")
                val usedWords = hashSetOf<String>()
                for (part in parts) {
                    if (part.isNotEmpty() && !usedWords.contains(part)) {
                        val count = words.getOrDefault(part, 0)
                        words[part] = count + 1
                        usedWords += part
                    }
                }
            }

            var maxWeight = -1
            var bestIndex = -1
            val delta = -1

            for (i in values.indices) {
                var weight = 0
                val value = filter2(values[i])
                val parts = value.split(" ")
                val usedWords = hashSetOf<String>()
                for (part in parts) {
                    if (part.isNotEmpty() && !usedWords.contains(part)) {
                        weight += specialWords.getOrDefault(part, words[part]!! + delta)

                        usedWords += part
                    }
                }
                if (weight > maxWeight) {
                    bestIndex = i
                    maxWeight = weight
                }
            }

            return bestIndex
        }

        private fun medianValue(values: List<String>): Int? {
            if (values.isEmpty()) {
                return null
            }
            if (values.size == 1) {
                return 0
            }

            var minDifference = 100000
            var bestIndex = -1

            for (i in values.indices) {
                var sumDiff = 0
                for (j in values.indices) {
                    if (i == j) continue
                    sumDiff += levenshtein(values[i], values[j])
                }

                if (sumDiff < minDifference) {
                    bestIndex = i
                    minDifference = sumDiff
                }
            }

            return bestIndex
        }

        private fun differentValues(values: List<String>): Boolean {
            var sumDiff = 0.0
            for (i in 0 until values.size - 1) {
                for (j in i + 1 until values.size) {
                    sumDiff += levenshtein(values[i], values[j])
                }
            }

            val countDiff = values.size * (values.size - 1) / 2
            if (countDiff > 0) sumDiff = 1.0 * sumDiff / countDiff
            return sumDiff > 2
        }

        private fun isTooShort(str: String, minLength: Int = 3): Boolean {
            return str.length < minLength
        }

        private fun filter2(str: String): String {
            return str.lowercase(Locale.getDefault())
                .replace("(", "")
                .replace(")", "")
                .replace(",", " ")
                .replace(".", " ")
                .replace("№", " ")
                .replace("  ", " ")
                .trim()
        }

        private fun isValid(str: String, field: String): Boolean {
            return field != "region" || str.none { it.isDigit() }
        }

        private fun filter(address: String): String {
            val result = StringBuilder()
            for (c in address) {
                if (c.isLetter())
                    result.append(c)
                else
                    result.append(" ")
            }

            val resultStr = result.toString().replace("  ", " ")

            return resultStr.lowercase(Locale.getDefault()).trim()
        }


        fun levenshtein(targetStr: String, sourceStr: String): Int {
            val m = targetStr.length
            val n = sourceStr.length
            val delta = Array(m + 1) { IntArray(n + 1) }
            for (i in 1..m)
                delta[i][0] = i
            for (j in 1..n)
                delta[0][j] = j
            for (j in 1..n)
                for (i in 1..m) {
                    if (targetStr[i - 1] == sourceStr[j - 1])
                        delta[i][j] = delta[i - 1][j - 1]
                    else
                        delta[i][j] = min(
                            delta[i - 1][j] + 1,
                            min(delta[i][j - 1] + 1, delta[i - 1][j - 1] + 1)
                        )
                }
            return delta[m][n]
        }

        fun prepareToCompare(value: String): String {
            var value = value.replace(',', ' ')
                .replace('.', ' ')
                .lowercase(Locale.getDefault())
            do {
                val len = value.length
                value = value.replace("  ", " ")
            } while (value.length < len)

            return value.trim()
        }

        private val KEYS = arrayOf(
            "region",
            "district",
            "city",
            "street",
            "house",
            "unit",
            "unit_type",
            "house_type",
            "street_type",
            "city_type",
            "district_type",
            "region_type",
            "postalcode"
        )
    }
}