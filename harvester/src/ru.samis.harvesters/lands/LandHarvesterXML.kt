package ru.samis.harvesters.lands

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.bson.conversions.Bson
import org.json.JSONObject
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.util.*
import javax.xml.parsers.SAXParserFactory

class LandHarvesterXML : LandHarvester() {

    var inserted = 0
    var updated = 0
    val refBook by lazy { JSONObject(File("refBook.json").readText()) }


    init {
        cadNumKey = "LABEL"
    }

    override fun parseSemantics(): Int {
        val files = (File(
            settings.getJSONObject("options").getString("catalog") + params.getString("xmlDir")
        ).listFiles { file -> file.name.endsWith("xml") })!!
        updated = 0
        inserted = 0

        for ((i, file) in files.withIndex()) {
            println("$i / ${files.size} ${file.name}")

            val factory = SAXParserFactory.newInstance()
            val parser = factory.newSAXParser()

            parser.parse(file, Handler())
        }

        return inserted
    }

    inner class Handler : DefaultHandler() {
        private var saveChars: ((String?) -> Unit)? = null
        private var inArea = 0
        private var street: String? = null
        private var locality: String? = null
        private var cadNum = ""
        private var cadNumInt = 0L


        private var updates = mutableListOf<Bson>()
        private var insertDoc = Document()

        override fun characters(ch: CharArray, start: Int, length: Int) {
            super.characters(ch, start, length)
            saveChars?.invoke(String(ch, start, length).trim())
        }

        @Throws(SAXException::class)
        override fun endElement(uri: String, localName: String, qName: String) {
            super.endElement(uri, localName, qName)
            saveChars = null
            when (qName.uppercase(Locale.getDefault())) {
                "AREA" -> inArea--

                "PARCEL" -> {
                    street = locality?.let { street?.let { "$street $locality" } ?: locality } ?: street
                    updates += Updates.set("Street", street)
                    updates += Updates.set("StreetID", street?.let { DigestUtils.sha1Hex(cadNum + street) })

                    if (isValidCadNum(cadNum)) {
                        if (updatesMap.keys.contains(cadNumInt)) {

                            val filteredDocument =
                                insertDoc.filter { updatesMap[cadNumInt]?.contains(it.key) == false }
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
                            for ((key, value) in insertDoc) {
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
                    }
                }
            }
        }

        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            when (qName.uppercase(Locale.getDefault())) {
                "PARCEL" -> {
                    insertDoc = Document()
                    updates.clear()
                    street = null
                    locality = null
                    cadNum = attributes.getValue("CadastralNumber")

                    if (isValidCadNum(cadNum)) {
                        cadNumInt = cadNum.replace("(", "")
                            .replace(")", "").replace(":", "").toLongOrNull()!!
                        val cadDigits = cadNum.split(":")

                        insertDoc.append("cadNumInt", cadNumInt)
                            .append("ID", cadNum)
                            .append(
                                "BlockID",
                                if (cadDigits.size >= 2)
                                    cadDigits.subList(0, 2).joinToString(":")
                                else cadNum
                            )
                            .append("Region", regionName)
                            .append("RegionID", regionID)
                            .append("RegionCode", regionCode)
                    }
                }
                "CADASTRALBLOCK" -> {
                    saveChars = {
                        insertDoc.append("CadQuarter", if (it != null && it.isNotBlank()) it else "")
                    }
                }

                "CADASTRALCOST" -> {
                    var cost = attributes.getValue("Value") ?: ""
                    insertDoc.append("Cost", cost.replace(",", ".").toDoubleOrNull())
                }

                "UTILIZATION" -> {
                    val allowed = attributes.getValue("PermittedUseText")
                    val allowedCode = attributes.getValue("Utilization")
                    insertDoc.append("Allowed", if (allowed != null && allowed.isNotBlank()) allowed else "")
                    insertDoc.append(
                        "AllowedCode",
                        if (allowedCode != null && allowedCode.isNotBlank()) allowedCode else ""
                    )
                }

                "LEVEL1" -> {
                    val type = attributes.getValue("Type")
                    val value = attributes.getValue("Value")
                    var houseNumber = ""
                    if (type != null && type.isNotBlank()) houseNumber += type
                    if (value != null && value.isNotBlank()) houseNumber += " $value"
                    if (houseNumber.isNotBlank()) insertDoc.append("HouseNumber", houseNumber.trim())

                }

                "CITY" -> {
                    val type = attributes.getValue("Type")
                    val name = attributes.getValue("Name")

                    var mun = ""

                    if (type != null && type.isNotBlank()) mun += type
                    if (name != null && name.isNotBlank()) mun += " $name"

                    if (mun.isNotBlank()) {
                        mun = mun.trim()
                        insertDoc.append("Municipalitet", mun)
                        insertDoc.append("MunicipalitetID", DigestUtils.sha1Hex(cadNum + mun))
                    }

                }

                "STREET" -> {
                    street = "${attributes.getValue("Type")} ${attributes.getValue("Name")}"
                }

                "LOCALITY" -> {
                    street = "${attributes.getValue("Type")} ${attributes.getValue("Name")}"
                }

                "AREA" -> {
                    inArea++
                    saveChars = {
                        if (inArea == 2) {
                            var square = it ?: ""
                            insertDoc.append("Square", square.replace(",", ".").toDoubleOrNull())
                        }
                    }
                }

                "OKATO" -> {
                    saveChars = {
                        insertDoc.append("OKATO", if (it != null && it.isNotBlank()) it else "")
                    }
                }

                "KLADR" -> {
                    saveChars = {
                        insertDoc.append("Cladr", if (it != null && it.isNotBlank()) it else "")
                    }
                }

                "NOTE" -> {
                    saveChars = {
                        insertDoc.append("AddressDesc", if (it != null && it.isNotBlank()) it else "")
                    }
                }

                "CADASTRALNUMBER" -> {
                    if (connectOKS) {
                        saveChars = {
                            if (it != cadNum && it != null) {
                                val regex = Regex("\\d{2}:\\d{2}:(\\d{6,7}):\\d+")
                                val oksSet = regex.findAll(it)
                                    .map { it.value }
                                    .toMutableSet()

                                connections.compute(cadNum) { _, existingSet ->
                                    (existingSet ?: mutableSetOf()).apply { addAll(oksSet) }
                                }
                            }
                        }
                    }
                }

                "CATEGORY" -> {
                    saveChars = {
                        val assignations = refBook.get("categories") as JSONObject
                        insertDoc.append("CategoryCode", it)
                            .append("Category", assignations.optString(it, ""))
                    }
                }

            }

        }
    }

    companion object {
        val requiredFields = listOf("ID", "cadNumInt", "BlockID", "Region", "RegionCode", "Geometry")
    }
}