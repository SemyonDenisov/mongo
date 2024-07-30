package ru.samis.harvesters

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import org.bson.Document
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.util.*
import javax.xml.parsers.SAXParserFactory


class GarStreetsParser : Harvester() {
    //    private val regionName = settings.getJSONObject("params").getString("region")
//    private val regionID = DigestUtils.sha1Hex(regionName)
    private val aotCollection = Array(18) {
        mutableMapOf<String, String>()
    }
    override val housesCollection = db.getCollection("gar_streets")

    override fun mainHarvest(): Int {

        try {
//            insertMetadata(datasetStructure)
//            aotCollection.drop()
//            addrObjCollection.drop()
//            itemsCollection.drop()
            housesCollection.drop()
            houseTypes.clear()
            val factory = SAXParserFactory.newInstance()
            val parser = factory.newSAXParser()

            val dir = settings.getJSONObject("options").getString("FiasCatalog")
            val regionDir = "$dir$regionCode/"
            var addrObjFile: File? = null
            var housesFile: File? = null
            var houseTypeFile: File? = null
            var aotFile: File? = null
            var admFile: File? = null
            var munFile: File? = null
            for (file in File(dir).listFiles()) {
                if (file.name.startsWith("AS_ADDR_OBJ_TYPES")) {
                    aotFile = file
                }
                if (file.name.startsWith("AS_HOUSE_TYPES")) {
                    houseTypeFile = file
                }
            }
            aotFile ?: run {
                writeError("AS_ADDR_OBJ_TYPES...XML not found")
                println("AS_ADDR_OBJ_TYPES...XML not found")
                return -1
            }
            for (file in File(regionDir).listFiles()) {
                if (file.name.startsWith("AS_ADDR_OBJ_") && file.name[12].isDigit()) {
                    addrObjFile = file
                }
                if (file.name.startsWith("AS_HOUSES_") && file.name[10].isDigit()) {
                    housesFile = file
                }
                if (file.name.startsWith("AS_ADM_HIERARCHY")) {
                    admFile = file
                }
                if (file.name.startsWith("AS_MUN_HIERARCHY")) {
                    munFile = file
                }
            }
            admFile ?: run {
                writeError("AS_ADM_HIERARCHY...XML not found")
                println("AS_ADM_HIERARCHY...XML not found")
                return -1
            }
//            munFile ?: run {
//                writeError("AS_MUN_HIERARCHY...XML not found")
//                println("AS_MUN_HIERARCHY...XML not found")
//                return
//            }
            addrObjFile ?: run {
                writeError("AS_ADDR_OBJ...XML not found")
                println("AS_ADDR_OBJ...XML not found")
                return -1
            }
            housesFile ?: run {
                writeError("AS_HOUSES...XML not found")
                println("AS_HOUSES...XML not found")
                return -1
            }


            Handler().apply {
                parser.parse(addrObjFile, this)
                housesCollection.createIndex(Indexes.ascending("OBJECTGUID"))
                housesCollection.createIndex(Indexes.ascending("OBJECTID"))
                housesCollection.createIndex(Indexes.ascending("ID"))
                println("AS_ADDR_OBJ processed")
                writeProgress(0.09, 0)


                parser.parse(aotFile, this)
                println("AS_ADDR_OBJ_TYPES processed")
                writeProgress(0.1, 0)

                parser.parse(admFile, this)
//                parser.parse(munFile, this)
                println("AS_ADM_HIERARCHY processed")

                writeProgress(1.0, streetsCount)

//                aotCollection.drop()
//                itemsCollection.drop()
//                addrObjCollection.drop()


            }
        } catch (e: Exception) {
            writeError(e.message ?: "")
            println(e.message ?: "")
        }
        return streetsCount
    }


    var streetsCount = 0
    var addrObjCount = 0
    var itemsCount = 0
    val date = Date()
    val houseTypes = hashMapOf<Int, String>()

    inner class Handler : DefaultHandler() {
        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            try {
                val document = Document()
                when (qName.toLowerCase()) {
                    "addressobjecttype" -> {
                        if (attributes.getValue("ISACTIVE").toLowerCase() != "true") return

                        val level = attributes.getValue("LEVEL").toIntOrNull() ?: 0
                        val short = attributes.getValue("SHORTNAME") ?: ""
                        aotCollection[level][short] = attributes.getValue("NAME")
                    }

                    "object" -> {
                        addrObjCount++
                        if (addrObjCount % 10000 == 0) println("$addrObjCount ADDROBJ processed")
                        if (attributes.getValue("ISACTUAL") != "1") return
                        if (attributes.getValue("ISACTIVE") != "1") return
                        for (objectField in objectFields) {
                            document.append(objectField, attributes.getValue(objectField))
                        }
                        housesCollection.insertOne(document)
//                        addrObjGuids.add(attributes.getValue("OBJECTGUID"))
                    }

                    "item" -> {
                        itemsCount++
                        if (attributes.getValue("ISACTIVE") != "1") return

                        val objectId = attributes.getValue("OBJECTID")
                        housesCollection.find(
                            Filters.eq("OBJECTID", objectId)
                        ).first()?.let { addrObject ->
                            val level = addrObject["LEVEL"]?.toString()?.toIntOrNull() ?: return@let
                            if (level < 7) return@let
                            val path = attributes.getValue("PATH").split(".")
                            var blockIDfound = false
                            addrObject.append("MunicipalitetID", null)
                            addrObject.append("BlockID", null)
                            for (id in path) {
                                if (id == objectId) continue

                                val doc = housesCollection.find(Filters.eq("OBJECTID", id)).first() ?: continue
                                val level = doc.getString("LEVEL").toIntOrNull() ?: 0
                                val fullTypename = aotCollection[level][doc["TYPENAME"]] ?: ""

                                val name = doc["NAME"].toString()

                                val key = when (level) {
                                    1 -> {
                                        addrObject
                                            .append("Region", name)
                                            .append("RegionType", fullTypename)

                                        "RegionID"
                                    }
                                    in 2..6 -> {
                                        addrObject.append("Municipalitet", name)
                                        addrObject.append("MunicipalitetType", fullTypename)
                                        if (blockIDfound)
                                            "MunicipalitetID"
                                        else {
                                            blockIDfound = true
                                            addrObject.append("MunicipalitetID", doc["OBJECTGUID"])
                                            "BlockID"
                                        }
                                    }
                                    7 -> {
                                        addrObject.append("Territory", name)
                                        addrObject.append("TerritoryType", fullTypename)
                                        "TerritoryID"
                                    }
                                    8 -> {
                                        addrObject.append("Street", name)
                                        "StreetID"
                                    }
                                    else -> ""
                                }
                                if (key.isNotBlank())
                                    addrObject.append(key, doc["OBJECTGUID"])
                            }

                            housesCollection.replaceOne(
                                Filters.eq("_id", addrObject["_id"]),
                                addrObject
                            )

                            streetsCount++
                            if (streetsCount % 10000 == 0) {
                                println("$streetsCount streets processed")
                            }

                        }

                        itemsCount++
                        if (itemsCount % 10000 == 0) {
                            println("$itemsCount items processed")
                            var progress = itemsCount / 500000.0 * 0.5 + 0.5
                            if (progress > 0.99) progress = 0.99
                            writeProgress(progress, itemsCount)
                        }
                    }

                    else -> return
                }
            } catch (e: Exception) {
                writeError(e.message ?: "")
            }

        }
    }

    companion object {
        val aotFields = listOf("ID", "LEVEL", "SHORTNAME", "NAME", "DESC")

        //        val itemFields = listOf(
//            "PARENTOBJID", "OBJECTID", "ID", "OKTMO", "PATH",
//            "REGIONCODE", "AREACODE", "CITYCODE", "PLACECODE", "PLANCODE", "STREETCODE"
//        )
        val objectFields = listOf(
            "OBJECTGUID", "OBJECTID", "ID", "TYPENAME", "NAME", "LEVEL"
        )
        val datasetStructure = arrayOf(
            arrayOf("ID", "ID", "string"),
            arrayOf("Region", "Регион", "string"),
            arrayOf("RegionID", "ID региона", "string"),
            arrayOf("Municipalitet", "Населённый пункт", "string"),
            arrayOf("MunicipalitetID", "ID населённого пункта", "string"),
            arrayOf("Street", "Улица", "string"),
            arrayOf("StreetID", "ID улицы", "string"),
            arrayOf("PostalCode", "Почтовый индекс", "string"),
            arrayOf("BlockID", "Район", "string"),
            arrayOf("HouseNumber", "Номер дома", "string"),
            arrayOf("x", "x", "float"),
            arrayOf("y", "y", "float"),
            arrayOf("Geometry", "Геометрия", "geometry")
        )
    }

}