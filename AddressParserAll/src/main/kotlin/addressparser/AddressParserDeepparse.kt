package ru.samis.addressparser

import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.*
import kotlin.system.exitProcess

class AddressParserDeepparse(
    parserDir: String,
    datasetPath: String,
    outFile: String
) : AddressParserBase(parserDir, datasetPath, outFile) {
    private val bucketFileName = "bucket${System.nanoTime()}.csv"
    private val bucketFilePath = "${parserDir}$bucketFileName"
    private val bucketOutFileName = "${File(bucketFileName).nameWithoutExtension}_parsed_address.json"
    private val bucketOutFilePath = "${parserDir}$bucketOutFileName"

    private val runCommand = options.getString("deepparseCommand")

    private val cleanTemp = options.optBoolean("cleanTempDeepparse", true)

    override val externalCommand: String = runCommand

    // "./bin/parse bpemb ./$bucketFileName $bucketOutFileName --path_to_retrained_model ./checkpoint/checkpoint.ckpt --csv_column_name Address"

    init {
        builder = ProcessBuilder(listOf("sh", externalCommand, bucketFileName, bucketOutFileName)).directory(File(parserDir))

    }


    override fun preRun() {
        super.preRun()
        FileWriter(bucketFilePath).use { writer ->
            writer.write("Address\n")
            for (address in addresses) {
                writer.write(address.optString("address"))
                writer.write("\n")
            }
        }

    }

    override val FIELDS_TRANSLATION = mapOf(
        "postalcode" to arrayOf("PostalCode"),
        "region" to arrayOf("Province"),
        "city" to arrayOf("Municipality"),
        "street" to arrayOf("StreetName"),
        "house" to arrayOf("StreetNumber"),
        "unit" to arrayOf("Unit")
    )

    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(bucketOutFilePath).delete()
            File(bucketFilePath).delete()
        }
    }

    override fun readResults(stdOut: String): List<JSONObject> {

        var result: MutableList<JSONObject> = JSONArray(FileReader(bucketOutFilePath).readText()).run {
            List(length()) { i ->
                getJSONObject(i)
            }
        }.toMutableList()


        if (result.size == 0 && addresses.size != 0) {
            while (result.size != addresses.size) {
                result.add(JSONObject())
            }
        }

        return result

    }
}