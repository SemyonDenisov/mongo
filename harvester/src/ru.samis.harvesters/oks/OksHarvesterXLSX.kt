package ru.samis.harvesters.oks

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import com.monitorjbl.xlsx.StreamingReader
import org.apache.commons.codec.digest.DigestUtils
import org.apache.poi.ss.usermodel.Row
import org.bson.Document
import org.bson.conversions.Bson
import org.json.JSONArray
import java.io.FileInputStream
import java.util.*
import kotlin.math.min

class OksHarvesterXLSX : OksHarvester() {

    class FieldInfo(var name: String, var type: String) {
        val indexes = mutableListOf<Int>()
        val prefixes = mutableListOf<String>()
    }

    override fun parseSemantics(): Int {

        val fileNames = params.getJSONArray("xlsx").run {
            Array(length()) { i ->
                getString(i)
            }
        }
        val sheetIndexes = params.getJSONArray("sheetIndexes").run {
            Array(length()) { i ->
                getInt(i)
            }
        }
        val firstRowIndexes = params.getJSONArray("firstRowIndexes").run {
            Array(length()) { i ->
                getInt(i)
            }
        }

        val dir = settings.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        val fields = params.getJSONArray("fields").let { fieldsJson ->
            Array(fieldsJson.length()) { fileIndex ->

                fieldsJson.getJSONArray(fileIndex).let { fileFields ->

                    mutableMapOf<Int, FieldInfo>().also { map ->

                        for (fieldIndex in 0 until fileFields.length()) {

                            fileFields.optJSONArray(fieldIndex)?.also { fieldDescr ->

                                fieldDescr.getJSONArray(1).let { indexes ->
                                    val prefixes = fieldDescr.optJSONArray(3) ?: JSONArray()
                                    val descr = FieldInfo(fieldDescr.optString(0), fieldDescr.optString(2))
                                    for (i in 0 until indexes.length()) {
                                        with(indexes.getInt(i)) {
                                            descr.indexes += this
                                            descr.prefixes += prefixes.optString(i, if (i == 0) "" else " ")
                                            map += this to descr
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for ((fileIndex, fileName) in fileNames.withIndex()) {
            println(dir + fileName)

            val fileFields = fields[min(fileIndex, fields.size - 1)]
            val firstRowIndex = firstRowIndexes[min(fileIndex, fields.size - 1)]
            val sheetIndex = sheetIndexes[min(fileIndex, fields.size - 1)]

            val formattedTemplates = mutableMapOf<String, FieldInfo>()


            FileInputStream(dir + fileName).use {
                var time = -System.nanoTime()

                val workbook = StreamingReader.builder()
                    .rowCacheSize(10)    // number of rows to keep in memory (defaults to 10)
                    .bufferSize(4096)     // buffer size to use when reading InputStream to file (defaults to 1024)
                    .open(it)            // InputStream or File for XLSX file (required)
                time += System.nanoTime()
                println("opening ${time / 1e9}")

                workbook.use {
                    val sheet = workbook.getSheetAt(sheetIndex)
                    val pairs = mutableMapOf<String, Any>()
                    for ((rowIndex, row) in sheet.withIndex()) {
                        if (rowIndex < firstRowIndex) continue
                        pairs.clear()
                        for (cellIndex in 0 until row.lastCellNum) {
                            val cell = row.getCell(cellIndex, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
                            fileFields[cellIndex]?.let { fieldInfo ->
                                val cellAsStr = cell?.stringCellValue ?: ""
                                val cellValue = when (fieldInfo.type.uppercase(Locale.getDefault())) {
                                    "INT" -> cellAsStr.replace(",", ".").toDoubleOrNull()?.toInt() ?: 0
                                    "DOUBLE" -> cellAsStr.replace(",", ".").toDoubleOrNull() ?: 0.0
                                    "FORMATTED" -> {
                                        formattedTemplates[fieldInfo.name] = fieldInfo
                                        ((pairs[fieldInfo.name] as MutableMap<Int, String>?) ?: mutableMapOf()).apply {
                                            this += cellIndex to cellAsStr
                                        }
                                    }
                                    else -> cellAsStr
                                }
                                pairs += fieldInfo.name to cellValue
                            }
                        }

                        if (!isValidCadNum(pairs["cadNum"] as String?)) continue
                        val cadNum = (pairs["cadNum"] as String?)
                            ?.replace("\n", "")
                            ?.replace("\"", "")
                            ?.replace(".", ":") ?: continue
                        pairs.remove("cadNum")

                        val cadNumInt = cadNum
                            .replace(":", "")
                            .replace("(", "")
                            .replace(")", "")
                            .toLongOrNull()

                        cadNumInt?.let {
                            val cadDigits = cadNum.split(":")
                            val updates = mutableListOf<Bson>()
                            for ((key, fieldInfo) in formattedTemplates) {
                                pairs[key]?.let { value ->
                                    val parts = value as Map<Int, String>
                                    var result = StringBuilder()
                                    for ((i, fieldIndex) in fieldInfo.indexes.withIndex()) {
                                        val fieldValue = parts[fieldIndex] ?: ""
                                        if (fieldValue.isNotBlank()) {
                                            val prefix = fieldInfo.prefixes[i]
                                            if (i > 0 && !prefix.startsWith(" ") && !result.endsWith(" ")) result.append(
                                                " "
                                            )
                                            result.append(prefix)
                                            if (!result.endsWith(" ")) result.append(" ")
                                            result.append(fieldValue)
                                        }
                                    }

                                    var resultStr = result.toString().trim()
                                    do {
                                        val len = resultStr.length
                                        resultStr = resultStr.replace("  ", " ")
                                    } while (resultStr.length != len)
                                    pairs[key] = resultStr
                                }
                            }

                            pairs["cadNumInt"] = cadNumInt
                            pairs["ID"] = cadNum
                            pairs["BlockID"] = if (cadDigits.size >= 2)
                                cadDigits.subList(0, 2).joinToString(":")
                            else cadNum

                            pairs["Region"] = regionName
                            pairs["RegionID"] = regionID
                            pairs["RegionCode"] = regionCode
                            if (pairs["Municipalitet"] != null) {
                                pairs["MunicipalitetID"] = DigestUtils.sha1Hex(pairs["Municipalitet"] as String)
                            }

                            if (pairs["Street"] != null) {
                                pairs["StreetID"] = DigestUtils.sha1Hex("${pairs["Municipalitet"]}${pairs["Street"]}")
                            }


                            if (updatesMap.keys.contains(cadNumInt)) {
                                val filteredDocument =
                                    pairs.filter { updatesMap[cadNumInt]?.contains(it.key) == false }
                                        .filter { !requiredFields.contains(it.key) }

                                filteredDocument.map {
                                    if (it.value != "") {
                                        updates += Updates.set(it.key, it.value)
                                    }
                                }
                                if (updateCollections) {
                                    housesCollection.updateOne(
                                        Filters.eq("cadNumInt", cadNumInt),
                                        Updates.combine(updates),
                                        UpdateOptions().upsert(true)
                                    )
                                }
                                updated++
                                if (updated % 1000 == 0) writeUpdateProgress(updated, inserted)

                            } else {
                                var doc = Document()
                                for ((key, value) in pairs) {
                                    if (value != "") {
                                        doc[key] = value
                                    }
                                }
                                if (updateCollections) housesCollection.insertOne(doc)
                                inserted++
                                incCount()
                                updated++
                                if (updated % 1000 == 0) writeUpdateProgress(updated, inserted)
                            }
                            updates.clear()
                            true
                        } ?: System.err.println(pairs)

                    }
                }
            }
        }

        return inserted
    }

    companion object {
        val requiredFields = listOf("ID", "cadNumInt", "BlockID", "Region", "RegionCode", "Geometry")
    }
}