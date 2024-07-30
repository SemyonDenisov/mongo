package ru.samis.harvesters

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import org.bson.Document
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.io.IOException
import java.nio.file.Paths
import java.util.*
import java.util.zip.ZipFile
import javax.xml.parsers.SAXParserFactory


class GarParser : Harvester() {
    //    private val regionName = settings.getJSONObject("params").getString("region")
//    private val regionID = DigestUtils.sha1Hex(regionName)
    private val aotCollection = db.getCollection("temp_gar_addressObjectTypes")
    private val addrObjCollection = db.getCollection("temp_addrObjects")
    private val itemsCollection = db.getCollection("temp_items_mun")


    var housesCount = 0
    var addrObjCount = 0
    var itemsCount = 0
    val date = Date()
    val houseTypes = hashMapOf<Int, String>()

    val houseParams = hashMapOf<Int, Document>()


    override fun mainHarvest(): Int {
//        val dgis = client
//            .getDatabase("region66_sverdlovskaya_obl").getCollection("2gis_houses")
//        val string = dgis.distinct("Municipalitet", String::class.java).toList().joinToString("\n")

        try {
            insertMetadata(datasetStructure)

            aotCollection.drop()
            addrObjCollection.drop()
            itemsCollection.drop()

            housesCollection.drop()
            houseTypes.clear()
            val factory = SAXParserFactory.newInstance()
            val parser = factory.newSAXParser()

            val dir = settings.getJSONObject("options").getString("FiasCatalog")
            // TODO
            // windows
            //val regionDir = "$dir$regionCode\\"
            // linux
            val regionDir = "$dir$regionCode/"

            var addrObjFile: File? = null
            var housesFile: File? = null
            var houseTypeFile: File? = null
            var aotFile: File? = null
            var admFile: File? = null
            var munFile: File? = null
            var housesParamsFile: File? = null


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
            //
            houseTypeFile ?: run {
                writeError("AS_HOUSE_TYPES...XML not found")
                println("AS_HOUSE_TYPES...XML not found")
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
                if (file.name.startsWith("AS_HOUSES_PARAMS_")) {
                    housesParamsFile = file
                }
            }
            admFile ?: run {
                writeError("AS_ADM_HIERARCHY...XML not found")
                println("AS_ADM_HIERARCHY...XML not found")
                return -1
            }
            munFile ?: run {
                writeError("AS_MUN_HIERARCHY...XML not found")
              println("AS_MUN_HIERARCHY...XML not found")
                return -1
            }
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
            housesParamsFile ?: run {
                writeError("AS_HOUSE_PARAMS...XML not found")
                println("AS_HOUSE_PARAMS...XML not found")
                return -1
            }

            Handler().apply {

                parser.parse(housesParamsFile, this)
                println("AS_HOUSES_PARAMS processed")
//парсим иерархию адресов: либо административное, либо муниципальное деление
               // parser.parse(admFile, this)
                //println("AS_ADM_HIERARCHY processed")

                parser.parse(munFile, this)
                println("AS_MUN_HIERARCHY processed")

                itemsCollection.createIndex(Indexes.ascending("ID"))
                itemsCollection.createIndex(Indexes.ascending("OBJECTID"))
                itemsCollection.createIndex(Indexes.ascending("PARENTOBJID"))
                writeProgress(0.4, 0)

                parser.parse(addrObjFile, this)
                addrObjCollection.createIndex(Indexes.ascending("OBJECTGUID"))
                addrObjCollection.createIndex(Indexes.ascending("OBJECTID"))
                addrObjCollection.createIndex(Indexes.ascending("ID"))
                println("AS_ADDR_OBJ processed")
                writeProgress(0.49, 0)

                parser.parse(houseTypeFile, this)
                parser.parse(aotFile, this)
                aotCollection.createIndex(Indexes.ascending("LEVEL"))
                aotCollection.createIndex(Indexes.ascending("ID"))
                aotCollection.createIndex(Indexes.ascending("SHORTNAME"))
                println("AS_ADDR_OBJ_TYPES processed")
                writeProgress(0.5, 0)

                parser.parse(housesFile, this)
                println("HOUSES processed")

                with(housesCollection) {
                    createIndex(Indexes.ascending("ID"))
                    createIndex(Indexes.ascending("RegionID"))
                    createIndex(Indexes.ascending("MunicipalitetID"))
                    createIndex(Indexes.ascending("StreetID"))
                    createIndex(Indexes.ascending("BlockID"))
                    createIndex(
                        Indexes.compoundIndex(
                            Indexes.text("Region"),
                            Indexes.text("Municipalitet"),
                            Indexes.text("Street"),
                            Indexes.text("HouseNumber")
                        )
                    )
                }
            }
        } catch (e: Exception) {
            writeError(e.message ?: "")
            println(e.message ?: "")
            return -1
        }
        return housesCount
    }


    inner class Handler : DefaultHandler() {
        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            try {
                val document = Document()
                var paramsDoc = Document()
                when (qName.toLowerCase()) {

                    "param" -> {
                        val objectId = attributes.getValue("OBJECTID").toInt()
                        val typeId = attributes.getValue("TYPEID")
                        val changeIdEnd = attributes.getValue("CHANGEIDEND")

                        if (changeIdEnd == "0") {
                            when (typeId) {
                                "8" -> {
                                    val value = attributes.getValue("VALUE")
                                    if (!value.isNullOrEmpty()) {
                                        paramsDoc.append("cadNum", value)
                                    }
                                }
                                "20" -> {
                                    val value = attributes.getValue("VALUE")
                                    if (!value.isNullOrEmpty()) {
                                        paramsDoc.append("CadRegFinished", value)
                                    }
                                }
                                "5" -> {
                                    val value = attributes.getValue("VALUE")
                                    if (!value.isNullOrEmpty()) {
                                        paramsDoc.append("PostalCode", value)
                                    }
                                }
                                "19" -> {
                                    val value = attributes.getValue("VALUE")
                                    if (!value.isNullOrEmpty()) {
                                        paramsDoc.append("MKD", value.toInt())
                                    }
                                }
                            }
                        }
                        if (paramsDoc.isNotEmpty()) {
                            houseParams.computeIfAbsent(objectId) { Document() }.putAll(paramsDoc)
                        }
                        paramsDoc.clear()
                    }


                    "addressobjecttype" -> {
                        if (attributes.getValue("ISACTIVE").toLowerCase() != "true") return
                        for (aotField in aotFields) {
                            document.append(aotField, attributes.getValue(aotField))
                        }
                        aotCollection.insertOne(document)
                    }


                    "object" -> {
                        addrObjCount++
                        if (addrObjCount % 10000 == 0) println("$addrObjCount ADDROBJ processed")
                        if (attributes.getValue("ISACTUAL") != "1") return
                        if (attributes.getValue("ISACTIVE") != "1") return
                        for (objectField in objectFields) {
                            document.append(objectField, attributes.getValue(objectField))
                        }
                        addrObjCollection.insertOne(document)
//                        addrObjGuids.add(attributes.getValue("OBJECTGUID"))
                    }

                    "item" -> {
                        itemsCount++
                        if (attributes.getValue("ISACTIVE") != "1") return
                        for (field in itemFields) {
                            attributes.getValue(field)?.let {
                                document.append(field, it)
                            }
                        }
                        itemsCollection.insertOne(document)
                        if (itemsCount % 10000 == 0) {
                            println("$itemsCount items processed")
                            var progress = itemsCount / 5500000.0 * 0.49
                            if (progress > 0.49) progress = 0.49
                            writeProgress(progress, 0)
                        }
                    }

                    "housetype" -> {
                        houseTypes[attributes.getValue("ID").toInt()] = attributes.getValue("NAME")
                    }

                    "house" -> {
                        val objectId = attributes.getValue("OBJECTID")
                        if (attributes.getValue("ISACTUAL") != "1") return
                        if (attributes.getValue("ISACTIVE") != "1") return

                        val hierarchy =
                            itemsCollection.find(
                                Filters.eq(
                                    "OBJECTID", attributes.getValue("OBJECTID")
                                )
                            ).first()
                                ?: return

                        var mun = ""
                        var street = ""
                        var region = ""
                        var blockIDfound = false
                        var solid = 0
                        document.append("StreetID", "empty")
                        document.append("MunicipalitetID", "empty")
                        document.append("BlockID", "empty")
                        hierarchy.getString("PATH").split(".").forEach { id ->
                            val doc = addrObjCollection.find(Filters.eq("OBJECTID", id)).first() ?: return@forEach
                            val fullTypename =
                                aotCollection.find(
                                    Filters.and(
                                        Filters.eq("SHORTNAME", doc["TYPENAME"]),
                                        Filters.eq("LEVEL", doc["LEVEL"])
                                    )
                                ).first()?.let {
                                    it["NAME"] ?: ""
                                } ?: doc["TYPENAME"] ?: ""


                            val text = "$fullTypename ${doc["NAME"]}"

                            val level = doc.getString("LEVEL").toIntOrNull() ?: 0
                            val key = when (level) {
                                1 -> {
                                    region = text
                                    "RegionID"
                                }
                                in 2..6 -> {
                                    mun += " $text"
                                    if (level == 5) solid = 1
                                    if (blockIDfound)
                                        "MunicipalitetID"
                                    else {
                                        blockIDfound = true
                                        document.append("MunicipalitetID", doc["OBJECTGUID"])
                                        "BlockID"
                                    }
                                }
                                7, 8 -> {
                                    street += " $text"
                                    "StreetID"
                                }
                                else -> ""
                            }
                            if (key.isNotBlank())
                                document.append(key, doc["OBJECTGUID"])
                        }

                        var houseNum = attributes.getValue("HOUSENUM") ?: ""
//                        var houseNum = when (attributes.getValue("HOUSETYPE")) {
//                            "1" -> "Корпус "
//                            "2" -> "Строение "
////                            "3" -> "Сооружение "
//                            "4" -> "Литера "
//                            else -> ""
//                        }
                        when (attributes.getValue("ADDTYPE1")) {
                            "1" -> houseNum += " к ${attributes.getValue("ADDNUM1")}"
                            "2" -> houseNum += " стр ${attributes.getValue("ADDNUM1")}"
                            "3" -> houseNum += " сооружение ${attributes.getValue("ADDNUM1")}"
                            "4" -> houseNum += " лит ${attributes.getValue("ADDNUM1")}"
                        }

                        when (attributes.getValue("ADDTYPE2")) {
                            "1" -> houseNum += " к ${attributes.getValue("ADDNUM2")}"
                            "2" -> houseNum += " стр ${attributes.getValue("ADDNUM2")}"
                            "3" -> houseNum += " сооружение ${attributes.getValue("ADDNUM2")}"
                            "4" -> houseNum += " лит ${attributes.getValue("ADDNUM2")}"
                        }

                        mun = mun.trim()
                        street = street.trim()
                        document.append("ID", attributes.getValue("OBJECTGUID"))
                            .append("Region", region)
//                            .append("RegionID", regionID)
                            .append("Municipalitet", mun)
//                            .append("MunicipalitetID", DigestUtils.sha1Hex(blockID + mun))
//                            .append("BlockID", blockID)
                            .append("Solid", solid)
                            .append("Street", street)
//                            .append("StreetID", DigestUtils.sha1Hex(blockID + mun + street))
                            .append("HouseNumber", houseNum.trim())
                            .append("OKTMO", hierarchy["OKTMO"])
                            .append("PostalCode", null)
                            .append("x", null)
                            .append("y", null)
                            .append("Geometry", Document("type", "WTF").append("coordinates", null))

                        attributes.getValue("HOUSETYPE")?.let { type ->
                            val type = type.toInt()
                            document.append("HouseTypeID", type)
                                .append("HouseType", houseTypes[type])
                        }

                        if (houseParams.containsKey(objectId.toInt())) {
                            val paramsDoc = houseParams[objectId.toInt()]
                            if (paramsDoc != null) {
                                for ((key, value) in paramsDoc) {
                                    if (value != null && value != "") document.append(key, value)
                                }
                            }
                        }
                        housesCollection.insertOne(document)

                        housesCount++
                        if (housesCount % 10000 == 0) {
                            println("$housesCount HOUSES processed")
                            var progress = housesCount / 500000.0 * 0.5 + 0.5
                            if (progress > 0.99) progress = 0.99
                            writeProgress(progress, housesCount)
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
        val itemFields = listOf(
            "PARENTOBJID", "OBJECTID", "ID", "OKTMO", "PATH"
        )
        //        val itemFields = listOf(
//            "PARENTOBJID", "OBJECTID", "ID", "OKTMO", "PATH",
//            "REGIONCODE", "AREACODE", "CITYCODE", "PLACECODE", "PLANCODE", "STREETCODE"
//        )
        val objectFields = listOf(
            "OBJECTGUID", "OBJECTID", "ID", "TYPENAME", "NAME", "LEVEL"
        )
        val houseFields = listOf(
            "OBJECTGUID", "OBJECTID", "ID", "HOUSENUM",
            "ADDNUM1", "ADDNUM2", "HOUSETYPE", "ADDTYPE1", "ADDTYPE2"
        )
        val houseDisplayFields = listOf(
            "HOUSENUM", "ADDNUM1", "ADDNUM2", "ADDTYPE1", "ADDTYPE2"
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