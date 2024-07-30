package ru.samis.addressparser

import org.json.JSONObject
import java.io.File
import java.io.FileWriter

class AddressParserLibpostal(
    parserDir: String,
    datasetPath: String,
    outFile: String
) : AddressParserBase(parserDir, datasetPath, outFile) {
    private val bucketFileName = "${parserDir}dataset${System.nanoTime()}.txt"
    override val externalCommand: String = "${parserDir}bin/python3 ${parserDir}run.py $bucketFileName"

    private val cleanTemp = options.optBoolean("cleanTempLibpostal", true)


    init {
        builder = ProcessBuilder(listOf("sh", "-c", externalCommand)).directory(File(parserDir))
    }

    override fun preRun() {
        super.preRun()
        FileWriter(bucketFileName).use { writer ->
            for (address in addresses) {
                writer.write(address.optString("address"))
                writer.write("\n")
            }
        }
    }

    override val FIELDS_TRANSLATION: Map<String, Array<String>> = mapOf(
        "postalcode" to arrayOf("postcode"),
        "region" to arrayOf("country_region", "state"),
        "city" to arrayOf("city"),
        "street" to arrayOf("street", "road"),
        "house" to arrayOf("house_number"),
        "unit" to arrayOf("unit")
    )

    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(bucketFileName).delete()
        }
    }

}