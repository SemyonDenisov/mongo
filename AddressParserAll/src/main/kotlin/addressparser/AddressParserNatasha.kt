package ru.samis.addressparser

import com.mongodb.client.MongoClients
import org.apache.commons.lang3.StringEscapeUtils
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.util.*

class AddressParserNatasha : AddressParserBase {
    private val bucketFileName = "export${System.nanoTime()}.csv"

    private val runCommand = options.getString("natashaCommand")

    //override val externalCommand: String = "python3 $SCRIPT_FILE_NAME $bucketFileName"
    override val externalCommand: String = runCommand

    private val cleanTemp = options.optBoolean("cleanTempNatasha", true)


    init {
        // builder = ProcessBuilder(listOf("sh", "-c", externalCommand)).directory(File(parserDir))
        builder = ProcessBuilder(listOf("sh", externalCommand, bucketFileName)).directory(File(parserDir))
        println("natasha: ${builder.command()}")

    }

    override fun preRun() {
        super.preRun()
        FileWriter(parserDir + bucketFileName).use { writer ->
            writer.write("address\n")

            for (address in addresses) {
                writer.write(address.optString("address"))
                writer.write("\n")
            }
        }

        FileReader(parserDir + bucketFileName).use {
            if (it.readLines().size != addresses.size + 1) {
                println("FILE WRITE COUNT ERROR")
                println(addresses.joinToString("\n"))
            }
        }

//        FileWriter(parserDir + SCRIPT_FILE_NAME).use {
//            it.write(template.replace("%FILE%", parserDir + bucketFileName))
//        }
    }


//    private val dataset: MongoCollection<Document>
//    private val addressFields: MutableList<String>
//    private val addressFieldsOld: MutableList<String?>
//    private val keyField: String
//    private val addressFieldsOnly: Boolean
//    private val recognizeRegion: Boolean
//    private val settings: JSONObject = JSONObject(File("settings.json").readText())


    constructor(
        parserDir: String,
        datasetPath: String,
        outFile: String
    ) : super(parserDir, datasetPath, outFile) {

//        val db = MongoClients.create(settings.getString("ConnectionString"))
//            .getDatabase(settings.getString("database"))
//        this.dataset = db.getCollection(settings.getString("dataset"))
//        addressFields = mutableListOf(settings.optString("addressField", "AddressDesc"))
//        keyField = settings.optString("keyField", "cadNumInt")

//        addressFieldsOnly = false
//        addressFieldsOld = mutableListOf()
//        recognizeRegion = false
    }

    constructor(
        database: String,
        dataset: String,
        addressFields: MutableList<String>,
        addressFieldsOld: MutableList<String?>,
        keyField: String,
        addressFieldsOnly: Boolean = true,
        recognizeRegion: Boolean = true,
        parserDir: String,
        datasetPath: String = "",
        outFile: String = ""
    ) : super(parserDir, datasetPath, outFile) {
        val settings = JSONObject(File("settings.json").readText())

        val db = MongoClients.create(settings.getString("ConnectionString"))
            .getDatabase(database)
//        this.dataset = db.getCollection(dataset)
//        this.addressFields = addressFields
//        this.addressFieldsOld = addressFieldsOld
//        this.keyField = keyField
//        this.addressFieldsOnly = addressFieldsOnly
//        this.recognizeRegion = recognizeRegion
//        parserDir = settings.getString("geonormCatalog")
    }

    override fun readResults(stdOut: String): List<JSONObject> {
        val lines = stdOut.lines()
        val result = mutableListOf<JSONObject>()
        try {
            val count = lines[3].toInt()
            if (count != addresses.size) {
                println("RESPONSE COUNT ERROR $count, expected ${addresses.size}")
                println(addresses.map { it.getString("address") })
                println(lines.joinToString("\n"))
            }
            for (i in 4 until lines.size) {
                val line = lines[i]
                if (line.isBlank()) continue
                try {
                    result += JSONObject(StringEscapeUtils.escapeJson(line))
                } catch (e: JSONException) {
                    // TODO
                    result += JSONObject()
                    println("JSONException in line $line")
                }
            }
        } catch (e: Exception) {
            println("Exception during natasha results parsing: ${e.message}")
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

    override fun copyFromResult(srcAddress: JSONObject, result: JSONObject, index: Int) {
        if (!srcAddress.has("unit")) {
            srcAddress.put(
                "unit",
                extractFlat(result.optString("not_decompose"))
            )
        }
        if (!srcAddress.has("house")) {
            srcAddress.put(
                "house",
                extractHouse(srcAddress.getString("address"))
            )
        }

        if (!srcAddress.has("street")) {
            val streetParts = mutableListOf<String>()
            result.opt("street")?.apply {
                with(toString()) {
                    if (isNotBlank()) streetParts += this
                }
            }
            result.opt("location")?.apply {
                with(toString()) {
                    if (isNotBlank()) streetParts += this
                }
            }
            result.opt("street_type")?.apply {
                with(toString()) {
                    if (isNotBlank() && streetParts.size == 2) streetParts.add(0, this)
                }
            }
            result.opt("location_type")?.apply {
                with(toString()) {
                    if (isNotBlank()) streetParts.add(if (streetParts.size >= 2) 2 else 0, this)
                }
            }

            srcAddress.put("street", streetParts.joinToString(" "))
        }
    }

    override val FIELDS_TRANSLATION: Map<String, Array<String>> = mapOf(
        "not_decomposed" to arrayOf("not_decompose"),
        "region_type" to arrayOf("region_type"),
        "region" to arrayOf("region"),
        "city" to arrayOf("settlement"),
        "district" to arrayOf("municipality"),
        "district_type" to arrayOf("municipality_type"),
        "city_type" to arrayOf("settlement_type"),
        "street_type" to arrayOf("street_type")
    )

    override fun cleanTempFiles() {
        if (cleanTemp) {
            File(bucketFileName).delete()
        }
    }

    private fun extractHouse(address: String): String {
        val address = address.replace(".", "")
        val result = StringBuilder()
        val buffer = StringBuilder()
        var spacesCount = 0
        var wordLength = 0
        var isNumeric = true
        var containsDigits = false
        var containsLetters = false
        var containsDash = false
        var numericCount = 0
        for (i in address.length - 1 downTo 0) {
            val c = address[i]
            val isNotLetterOrDigit = !c.isLetterOrDigit()
            if (isNotLetterOrDigit && buffer.isBlank() && result.isBlank()) continue

            if (isNotLetterOrDigit && !HOUSE_LITER_SYMBOLS.contains(c)) {
                spacesCount++
                if (isNumeric) {
                    numericCount++
//                if (numericCount > 2) return result.toString().trim()
                }

                if (!containsDigits) {
                    val word = buffer.toString().lowercase(Locale.getDefault())
                    if (FLAT_REPLACEMENTS.find { it.first == word || it.second == word } != null) {
                        result.clear()
                        buffer.clear()
                    } else {
                        val isBlank = result.isBlank()
                        if (isBlank && wordLength > 1 || !isBlank && !HOUSE_PARTS_TEMPLATES.contains(word))
                            return result.toString().trim()
                    }
                }

                if (c == ',' && (result.length + buffer.length > 0)) {
                    result.insert(0, buffer)
                    return result.toString().trim()
                }

                isNumeric = true
                wordLength = 0
                containsDigits = false
                containsLetters = false
                containsDash = false

                result.insert(0, buffer)
                result.insert(0, " ")
                buffer.clear()
            } else {
                if (c.isDigit())
                    containsDigits = true
                else {
                    isNumeric = false
                }
                wordLength++
                buffer.insert(0, c)
            }
        }
        return ""
    }

    private fun extractFlat(notDecomposed: String): String {
        val notDecomposed = notDecomposed.lowercase(Locale.getDefault())
        for (template in FLAT_TEMPLATES) {
            val index = notDecomposed.indexOf(template)
            if (index >= 0) {
                val flatNumber = notDecomposed.substring(index + template.length).trim()
                val numberFiltered = StringBuilder()
                var digitFound = false
                for (c in flatNumber) {
                    if (c.isLetter()) {
                        numberFiltered.append(c)
                    } else if (c.isDigit()) {
                        numberFiltered.append(c)
                        digitFound = true
                    } else if (digitFound)
                        break
                }
//                for (replacement in FLAT_REPLACEMENTS) {
//                    flatNumber = flatNumber.replace(replacement.first, replacement.second)
//                }

                return numberFiltered.toString()
            }
        }
        return ""
    }


    companion object {
        val SCRIPT_FILE_NAME = "export.py"

        val DELIMITERS = hashSetOf(' ', '-', '"', '/', ',', ';', ':')
        val HOUSE_LITER_SYMBOLS = hashSetOf('-', '"', '/')
        val HOUSE_PARTS_TEMPLATES =
            hashSetOf("корп", "корпус", "лит", "литер", "литера", "часть", "стр", "ст", "строение")
        val HOUSE_TEMPLATES = arrayOf(",уч ", "уч ", "участок", "гараж", "№")
        val HOUSE = " дом "
        val SETTL_TEMPLATES = arrayOf("снт ", ",тер ", " тер ")
        val SETTL = " посёлок "
        val FLAT_TEMPLATES =
            arrayOf(
                "квартира",
                "кв.",
                "кв ",
                "помещение",
                "пом.",
                "пом ",
                "офис",
                "оф.",
                "оф ",
                "комната",
                "комнаты",
                "комн ",
                "комн.",
                "ком ",
                "ком."
            )
        val FLAT_REPLACEMENTS = arrayOf("квартира" to "кв")
        val PORTION_SIZE = 100

//        const val template = "import pathlib\n" +
//                "\n" +
//                "import pandas as pd\n" +
//                "\n" +
//                "from geonorm.core_test import decompose_expand\n" +
//                "\n" +
//                "from geonorm.geonormaliser_utils import decompose  # Импорт метода\n" +
//                "\n\n" +
//                "def load_df():\n" +
//                "    compression = None\n" +
//                "    df = pd.read_csv(\n" +
//                "        pathlib.Path(f'%FILE%'),\n" +
//                "        compression=compression,\n" +
//                "        sep='^',\n" +
//                "        decimal='.',\n" +
//                "        dtype={'n': int},\n" +
//                "        keep_default_na=True\n" +
//                "    ).fillna('')\n" +
//                "\n" +
//                "    return df\n" +
//                "\n" +
//                "\n" +
//                "test_set = load_df()\n" +
//                "X_dec = test_set['address'].parallel_apply(decompose)\n" +
//                "print(len(X_dec))\n" +
//                "for address in X_dec:\n" +
//                "    if address is not None and address != '':\n" +
//                "        print(address)\n" +
//                "    else:\n" +
//                "        print('')"
    }


    override fun preProcessInputAddress(address: String): String {
        var address = address.replace(".", " ").replace("\\", "\\\\")
            .replace("\"", "").replace("№", "").replace("\n", " ")
        while (address.indexOf("\n") >= 0)
            address = address.replace("\n", " ")
        var len = Int.MAX_VALUE
        while (len > address.length) {
            len = address.length
            address = address.replace("  ", " ")
        }

        var replaced = false
        var addressLower = address.lowercase(Locale.getDefault())
        for (houseTemplate in HOUSE_TEMPLATES) {
            val index = addressLower.indexOf(houseTemplate)
            if (index >= 0) {
                address = address.replaceRange(index, index + houseTemplate.length, if (replaced) "" else HOUSE)
                addressLower = address.lowercase(Locale.getDefault())
                replaced = true
            }
        }

        replaced = false
        for (settlTemplate in SETTL_TEMPLATES) {
            val index = addressLower.indexOf(settlTemplate)
            if (index >= 0) {
                address = address.replaceRange(index, index + settlTemplate.length, if (replaced) "" else SETTL)
                addressLower = address.lowercase(Locale.getDefault())
                replaced = true
            }
        }

        len = Int.MAX_VALUE
        while (len > address.length) {
            len = address.length
            address = address.replace("  ", " ")
        }

        return address
    }
}