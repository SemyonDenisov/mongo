package ru.samis.harvesters.lands

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.bson.Document
import org.bson.conversions.Bson
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.io.FileReader
import java.util.*
import javax.xml.parsers.SAXParserFactory

class LandHarvester02 : LandHarvester() {
    private var updated = 0
    private var inserted = 0
    override fun parseSemantics(): Int {


        val dir = settings.getJSONObject("options").getString("catalog")
       // val params = params.getJSONObject("params")

        parseCsv1(dir + params.getString("csv1"))
        parseCsv2(dir + params.getString("csv2"))
        parseXml(File(dir + params.getString("xmlDir")))


        return inserted
    }

    private fun parseXml(xmlDir: File) {
        val xmls = xmlDir.listFiles { file ->
            file.isDirectory || file.name.toLowerCase().endsWith("xml") }
        for (file in xmls) {
            println(file.name)
            if (file.isDirectory) {
                parseXml(file)
                continue
            }
            val factory = SAXParserFactory.newInstance()
            val parser = factory.newSAXParser()

            parser.parse(file, Handler())
        }
    }

    inner class Handler : DefaultHandler() {
        private var buffer = ""
        //        private lateinit var document: Document
        private val updates = mutableListOf<Bson>()
        private var city = ""
        private var locality = ""
        private var cadNumInt: Long? = null
        private var cadNum = ""


        override fun characters(ch: CharArray, start: Int, length: Int) {
            super.characters(ch, start, length)
            buffer = String(ch, start, length)
        }

        override fun endElement(uri: String, localName: String, qName: String) {
            super.endElement(uri, localName, qName)
            when (qName) {
                "Area" -> if (buffer.isNotBlank()) updates += Updates.set("Square", buffer.toDouble())

                "Address" -> updates += Updates.set("Municipalitet", "$city $locality")

                "DateValuation" -> updates += Updates.set("CadastrCostEstDate", buffer)

                "Parcel" -> {
                    if (housesCollection.updateOne(
                            Filters.eq("cadNumInt", cadNumInt),
                            Updates.combine(updates),
                            UpdateOptions().upsert(true)
                        ).upsertedId == null
                    ) {
                        inserted++
                        incCount()
                    }
                    updated++
                    if (updated % 10000 == 0) {
                        println("XML $updated updated")
                    }
                }
            }
            buffer = ""
        }

        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            buffer = ""
            try {
                when (qName) {
                    "Parcel" -> {
                        city = ""
                        locality = ""
                        cadNum = attributes.getValue("CadastralNumber")
                        cadNumInt = cadNum
                            .replace(":", "")
                            .replace("(", "")
                            .replace(")", "")
                            .toLongOrNull()
                        updates.clear()
                        updates += Updates.setOnInsert("cadNum", cadNum)
                        updates += Updates.set("Date", attributes.getValue("DateCreated"))
                    }

                    "CadastralCost" -> updates += Updates.set(
                        "CadastrCost",
                        attributes.getValue("Value").toDouble()
                    )


                    "Utilization" -> updates += Updates.set(
                        "AllowedDoc",
                        attributes.getValue("ByDoc")
                    )

//                    "District" -> updates += Updates.set(
//                        "BlockID",
//                        attributes.getValue("Name") + " " + attributes.getValue("Type")
//                    )

                    "Street" -> updates += Updates.set(
                        "Street",
                        attributes.getValue("Name") + " " + attributes.getValue("Type")
                    )

                    "City" -> city = attributes.getValue("Name") + " " + attributes.getValue("Type")

                    "Locality" -> locality = attributes.getValue("Name") + " " + attributes.getValue("Type")

                    "Level1" -> updates += Updates.set(
                        "Level1",
                        Document("type", attributes.getValue("Type")).append("name", attributes.getValue("Name"))
                    )

                    "Level2" -> updates += Updates.set(
                        "Level2",
                        Document("type", attributes.getValue("Type")).append("name", attributes.getValue("Name"))
                    )

                    "Level3" -> updates += Updates.set(
                        "Level3",
                        Document("type", attributes.getValue("Type")).append("name", attributes.getValue("Name"))
                    )

                    else -> return
                }
            } catch (e: Exception) {
                writeError(e.message ?: "")
            }

        }
    }

    private fun parseCsv1(fileName: String) {
        val inserter1 = { tokens: List<String> ->
            val cadNumInt = tokens[2]
                .replace(":", "")
                .replace("(", "")
                .replace(")", "")
                .toLongOrNull()

            cadNumInt?.let {
                inserted += if (housesCollection.updateOne(
                        Filters.eq("cadNumInt", cadNumInt),
                        Updates.combine(
//                            Updates.set("BlockID", null),
                            Updates.set("Municipalitet", null),
                            Updates.set("Street", null),
                            Updates.set("HouseNumber", null),
                            Updates.set("Flat", null),
                            Updates.set("Date", null),
                            Updates.set("Square", tokens[4]),
                            Updates.set("PostalCode", null),
                            Updates.set("AddressDesc", tokens[5]),
                            Updates.set("AllowedCode", tokens[7]),
                            Updates.set("AllowedClass", tokens[8]),
                            Updates.set("AllowedDoc", tokens[6]),
                            Updates.set("Allowed", null),
                            Updates.set("CadastrCost", null),
                            Updates.set("CadastrCostEstDate", null),
                            Updates.setOnInsert(
                                Document("cadNum", tokens[2])
                                    .append("Geometry", Document("type", "WTF").append("coordinates", null))
                                    .append("Region", regionName)
                                    .append("RegionID", regionID)
                                    .append("RegionCode", regionCode)
                            )
                        ),
                        UpdateOptions().upsert(true)
                    ).upsertedId == null
                ) 0 else 1
                updated++
                if (updated % 10000 == 0) {
                    println("CSV #1 $updated updated")
                    var progress = inserted / 1000000.0
                    if (progress > 0.99) progress = 0.99
                    writeProgress(progress, inserted)
                }
                true
            } ?: System.err.println(tokens)
        }

        FileReader(fileName).buffered().use { reader ->
            reader.readLine()

            var line = reader.readLine()
            var oldLine: String? = null
            while (line != null) {
                var tokens = line.split("^").toMutableList()
                oldLine?.let {
                    if (tokens.size > 1 && tokens[0].toIntOrNull() != null && tokens[1].contains(":")) {
                        val newTokens = it.split("^").toMutableList()

                        while (newTokens.size < 9) newTokens.add("")
                        inserter1(newTokens)
                        oldLine = null
                    } else {
                        oldLine += " $line"
                        tokens = oldLine!!.split("^").toMutableList()
                    }
                }

                if (tokens.size < 9) {
                    System.err.println(tokens)
                    oldLine = if (oldLine == null) line else "$oldLine $line"
                    line = reader.readLine()
                    continue
                }
                oldLine = null
                inserter1(tokens)
                line = reader.readLine()
            }
        }

    }


    private fun parseCsv2(fileName: String) {
        val inserter2 = { tokens: List<String> ->
            val cadNumInt = tokens[0]
                .replace(":", "")
                .replace("(", "")
                .replace(")", "")
                .toLongOrNull()

            cadNumInt?.let {
                inserted += if (housesCollection.updateOne(
                        Filters.eq("cadNumInt", cadNumInt),
                        Updates.combine(
                            Updates.set("BlockID", null),
                            Updates.set("Municipalitet", null),
                            Updates.set("Street", null),
                            Updates.set("HouseNumber", null),
                            Updates.set("Flat", null),
                            Updates.set("Date", tokens[20]),
                            Updates.set("Square", tokens[19]),
                            Updates.set("DeclaredSquare", tokens[18]),
                            Updates.set("PostalCode", null),
                            Updates.set("AddressDesc", tokens[8]),
                            Updates.set("AllowedCode", null),
                            Updates.set("AllowedClass", null),
                            Updates.set("AllowedDoc", null),
                            Updates.set("Allowed", tokens[16]),
                            Updates.set("CadastrCost", tokens[21]),
                            Updates.set("CadastrCostEstDate", tokens[24]),
                            Updates.setOnInsert(
                                Document("cadNum", tokens[0])
                                    .append("Geometry", Document("type", "WTF").append("coordinates", null))
                                    .append("Region", regionName)
                                    .append("RegionID", regionID)
                                    .append("RegionCode", regionCode)
                            )
                        ),
                        UpdateOptions().upsert(true)
                    ).upsertedId == null
                ) 0 else 1
                updated++
                if (updated % 10000 == 0) {
                    println("CSV #2 $updated updated")
                    var progress = inserted / 1000000.0
                    if (progress > 0.99) progress = 0.99
                    writeProgress(progress, inserted)
                }
                true
            } ?: System.err.println(tokens)
        }

        FileReader(fileName).buffered().use { reader ->
            reader.readLine()

            var line = reader.readLine()
            var oldLine: String? = null
            while (line != null) {
                var tokens = line.split("^").toMutableList()
                oldLine?.let {
                    if (tokens.size > 1 && tokens[0].toIntOrNull() != null && tokens[1].contains(":")) {
                        val newTokens = it.split("^").toMutableList()

                        while (newTokens.size < 25) newTokens.add("")
                        inserter2(newTokens)
                        oldLine = null
                    } else {
                        oldLine += " $line"
                        tokens = oldLine!!.split("^").toMutableList()
                    }
                }

                if (tokens.size < 25) {
                    System.err.println(tokens)
                    oldLine = if (oldLine == null) line else "$oldLine $line"
                    line = reader.readLine()
                    continue
                }
                oldLine = null
                inserter2(tokens)
                line = reader.readLine()
            }
        }
    }
}