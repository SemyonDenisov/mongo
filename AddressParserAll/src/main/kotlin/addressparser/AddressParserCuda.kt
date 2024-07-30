package ru.samis.addressparser

import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.*
import kotlin.system.exitProcess

class AddressParserCuda(
    parserDir: String,
    datasetPath: String,
    outFile: String
) : AddressParserBase(parserDir, datasetPath, outFile) {
    //private val scriptFileName: String = "${parserDir}run.py"
   // private val bucketFileName = "${parserDir}dataset${System.nanoTime()}.txt"
    private val bucketFileName = "dataset${System.nanoTime()}.txt"
    private val bucketFilePath = "$parserDir$bucketFileName"

    private val bucketOutFileName = "${"output_$bucketFileName"}"
    private val bucketOutFilePath = "$parserDir$bucketOutFileName"

    private val cleanTemp = options.optBoolean("cleanTempCuda", true)
    private val runCommand = options.getString("cudaCommand")

    override val externalCommand: String = runCommand

    init {
        builder = ProcessBuilder(listOf("sh", externalCommand, bucketFileName, bucketOutFileName)).directory(File(parserDir))
        println(builder.command())
    }

    override fun readResults(stdOut: String): List<JSONObject> {
        val result = mutableListOf<JSONObject>()
        File(bucketOutFilePath).forEachLine { line ->
            if (line.isNotBlank()) {
                try {
                    JSONArray(line.replace("\\", "\\\\")).apply {
                        result += if (length() > 0) getJSONObject(0) else JSONObject()
                    }
                } catch (e: JSONException) {
                    result += JSONObject()
                    println("JSONException in line $line")
                }
            }
        }

        if (result.size == 0 && addresses.size != 0) {
            while (result.size != addresses.size) {
                result.add(JSONObject())
            }
        }

//        println("cuda stdout\n$stdOut")
        /*
        for (line in stdOut.lines()) {
            if (line.isBlank()) continue
            try {
                JSONArray(line.replace("\\", "\\\\")).apply {
                    result += if (length() > 0) getJSONObject(0) else JSONObject()
                }
            } catch (e: JSONException) {
                result += JSONObject()
                println("JSONException in line $line")
            }
            println("LINE: $line")
        }
        */
        return result
    }

    override fun preRun() {
        super.preRun()
        FileWriter(bucketFilePath).use { writer ->
            for (address in addresses) {
                writer.write(address.optString("address"))
                writer.write("\n")
            }
        }

    }

    override fun copyFromResult(srcAddress: JSONObject, result: JSONObject, index: Int) {

        if (!srcAddress.has("city")) {
            val cityParts = mutableListOf<String>()
            result.opt("city")?.toString()?.apply {
                if (isNotBlank()) cityParts += this
            }
            result.opt("settlement")?.toString()?.apply {
                if (isNotBlank()) cityParts += this
            }
            result.opt("city_type")?.toString()?.apply {
                if (isNotBlank() && cityParts.size == 2) cityParts.add(0, this)
            }
            result.opt("settlement_type")?.toString()?.apply {
                if (isNotBlank() && cityParts.size >= 2) cityParts.add(2, this)
            }

            srcAddress.put("city", cityParts.joinToString(" "))
        }
    }

    fun parseToCsv() {
        val scriptFileName = "${parserDir}run.py"
        val pythonCmd = listOf(
            "sh",
            "-c",
            "python3 $scriptFileName"
        )

        val builder = ProcessBuilder(pythonCmd).directory(File(parserDir))

//        val keys = hashSetOf<String>()
        var i = 1
        val addresses = mutableListOf<String>()


        var time = -System.nanoTime()
        OutputStreamWriter(FileOutputStream("out.csv"), "windows-1251").use { writer ->
            writer.write("src;results_count")
            for (field in FIELDS) {
                writer.write(";")
                writer.write(field)
            }
            writer.write("\n")

            val processBasket = {
                println("processing basket ${i++}")
                FileWriter(scriptFileName).use { writer ->
                    val pythonAddresses = addresses.joinToString("','", "'", "'") { address ->
                        address.replace("'", "\"")
                    }

                    val script = SCRIPT.replace("%DIR%", parserDir)
                        .replace("%ADDRESSES%", pythonAddresses)
                    writer.write(script)
                }

                val runtimeProcess = builder.start()
//                runtimeProcess.waitFor()
                val outLines = runtimeProcess.inputStream.bufferedReader().readLines()
                var iAddr = -1
                for (line in outLines) {
                    try {
                        val result = JSONArray(line)
                        iAddr++
                        println(addresses[iAddr])
                        for (i in 0 until result.length()) {
                            val obj = result.getJSONObject(i)
                            writer.write(addresses[iAddr])
                            writer.write(";")
                            writer.write(result.length().toString())
                            for (field in FIELDS) {
                                writer.write(";")
                                writer.write(obj.opt(field)?.toString() ?: "")
                                obj.opt(field)?.apply {
                                    println("#$i $field: $this")
                                }
                            }
                            writer.write("\n")
                            writer.flush()
                        }
                        println()
                    } catch (e: Exception) {
                    }
                }
            }


            FileReader(datasetPath).forEachLine { line ->
                addresses += line

                if (addresses.size == 100) {
                    processBasket()

//                println(result)
                    addresses.clear()

                    println()
                }
            }

            if (addresses.isNotEmpty())
                processBasket()
        }


        time += System.nanoTime()
        println("time ${time / 1e6}")
    }

    override val FIELDS_TRANSLATION = mapOf(
        "postalcode" to arrayOf("zipcode"),
        "region_type" to arrayOf("region_type"),
        "region" to arrayOf("region"),
        "city_type" to arrayOf("city_type"),
        "city" to arrayOf("city"),
        "street_type" to arrayOf("street_type"),
        "street" to arrayOf("street"),
        "house_type" to arrayOf("house_type"),
        "house" to arrayOf("house_type", "house", "corpus_type", "corpus"),
        "unit" to arrayOf("rm"),
        "unit_type" to arrayOf("rm_type")
    )

    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(bucketFileName).delete()
            File(bucketOutFileName).delete()
        }
    }

    companion object {

        val FIELDS = arrayOf(
            "zipcode",
            "zipcode_type",
            "region",
            "region_type",
            "area",
            "area_type",
            "city",
            "city_type",
            "settlement",
            "settlement_type",
            "district",
            "district_type",
            "microdistrict",
            "microdistrict_type",
            "quarter",
            "quarter_type",
            "street",
            "street_type",
            "house",
            "house_type",
            "corpus",
            "corpus_type",
            "rm",
            "rm_type"
        )


        const val SCRIPT = "# coding=utf-8\n" +
                "\n" +
                "import sys\n" +
                "sys.path.append(\"%DIR%\")\n" +
                "\n" +
                "from address_parser import AddressParser\n" +
                "\n" +
                "parser = AddressParser()\n" +
                "\n" +
                "addresses = [%ADDRESSES%]\n" +
                "for address in addresses:\n" +
                "\t#print(address)\n" +
                "\tresult=parser(address)\n" +
                "\tprint(result)"
    }
}