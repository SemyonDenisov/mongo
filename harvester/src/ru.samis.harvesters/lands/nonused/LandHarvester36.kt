package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.bson.conversions.Bson
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import ru.samis.harvesters.lands.LandHarvester
import java.io.File
import java.util.*
import javax.xml.parsers.SAXParserFactory

class LandHarvester36 : LandHarvester() {
    var inserted = 0
    var updated = 0

    init {
        cadNumKey = "LABEL"
    }

    override fun parseSemantics(): Int {
        val files = (File(params.getString("xmlDir")).listFiles { file -> file.name.endsWith("xml") })!!
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
        private val updates = mutableListOf<Bson>()
        private var street: String? = null
        private var locality: String? = null
        private var cadNum = ""
        private val estCount = housesCollection.countDocuments() * 1.25

        override fun characters(ch: CharArray, start: Int, length: Int) {
            super.characters(ch, start, length)
//            println(String(ch, start, length).trim())
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

                    if (housesCollection.updateOne(
                            eq("cadNum", cadNum),
                            Updates.combine(updates),
                            UpdateOptions().upsert(true)
                        ).upsertedId != null
                    ) {
                        incCount()
                        inserted++
                    }

                    updated++
                    if (updated % 10000 == 0) {
                        println("$updated updated")
                        var progress = inserted / estCount
                        if (progress > 0.99) progress = 0.99
                        writeProgress(progress, inserted)
                    }
                }
            }
        }

        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            when (qName.uppercase(Locale.getDefault())) {
                "PARCEL" -> {
                    updates.clear()
                    street = null
                    locality = null
                    cadNum = attributes.getValue("CadastralNumber")
                    val cadNumInt = cadNum.replace("(", "")
                        .replace(")", "").replace(":", "").toLongOrNull()
                    val cadDigits = cadNum.split(":")
                    updates += Updates.setOnInsert(
                        Document()
                            .append("cadNum", cadNum)
                            .append("cadNumInt", cadNumInt)
                            .append(
                                "BlockID",
                                if (cadDigits.size >= 2)
                                    cadDigits.subList(0, 2).joinToString(":")
                                else cadNum
                            )
                            .append("Geometry", Document("type", "WTF").append("coordinates", null))
                            .append("Region", regionName)
                            .append("RegionID", regionID)
                            .append("RegionCode", regionCode)
                    )
                    updates += Updates.set("Date", attributes.getValue("DateCreated"))
                }

                "CADASTRALCOST" -> {
                    updates += Updates.set("CadastrCost", attributes.getValue("Value")?.toDoubleOrNull())
                }

                "DATEVALUATION" -> {
                    saveChars = { Updates.set("CadastrCostEstDate", it) }
                }

                "UTILIZATION" -> {
                    updates += Updates.set("Allowed", attributes.getValue("PermittedUseText"))
                    updates += Updates.set("AllowedCode", attributes.getValue("Utilization"))
                }

                "LEVEL1" -> {
                    updates += Updates.set(
                        "HouseNumber",
                        "${attributes.getValue("Type")} ${attributes.getValue("Value")}"
                    )
                }

                "CITY" -> {
                    val mun = "${attributes.getValue("Type")} ${attributes.getValue("Name")}"
                    updates += Updates.set("Municipalitet", mun)
                    updates += Updates.set("MunicipalitetID", DigestUtils.sha1Hex(cadNum + mun))
                }

                "STREET" -> {
                    street = "${attributes.getValue("Type")} ${attributes.getValue("Name")}"
                }

                "LOCALITY" -> {
                    street = "${attributes.getValue("Type")} ${attributes.getValue("Name")}"
                }

                "AREA" -> {
                    inArea++
                    if (inArea == 2) {
                        saveChars = { updates += Updates.set("Square", it?.toDoubleOrNull()) }
                    }
                }

                "OKATO" -> {
                    saveChars = { updates += Updates.set("OKATO", it) }
                }

                "KLADR" -> {
                    saveChars = { updates += Updates.set("Cladr", it) }
                }

                "NOTE" -> {
                    saveChars = { updates += Updates.set("AddressDesc", it) }
                }

            }

        }
    }
}