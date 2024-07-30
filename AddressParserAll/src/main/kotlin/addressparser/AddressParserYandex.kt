package ru.samis.addressparser

import org.json.JSONObject
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class AddressParserYandex(
    parserDir: String,
    datasetPath: String,
    outFile: String
) : AddressParserBase(parserDir, datasetPath, outFile) {
    private val bucketFileName = "${parserDir}bucket${System.nanoTime()}.txt"
    private val bucketOutFile = "${bucketFileName}_results.txt"

    private val cleanTemp = options.optBoolean("cleanTempYandex", true)

    override val externalCommand: String = "python3 geocoder.py $bucketFileName $bucketOutFile"

    init {
        builder = ProcessBuilder(listOf("sh", "-c", externalCommand)).directory(File(parserDir))
    }

    // override val externalCommand: String = "python3 geocoder.py $bucketFileName $bucketOutFile"

    override fun preRun() {
        super.preRun()
        FileWriter(bucketFileName).use { writer ->
            for (address in addresses) {
                writer.write(address.optString("address"))
                writer.write("\n")
            }
        }
    }

    override val FIELDS_TRANSLATION = EMPTY_FIELDS_TRANSLATION
    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(bucketFileName).delete()
            File(bucketOutFile).delete()
        }
    }

    override fun readResults(stdOut: String): List<JSONObject> {
        val lines = FileReader(bucketOutFile).readLines()
        return List(lines.size) { i ->
            JSONObject(lines[i])
        }
    }
}