package ru.samis.harvesters

import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Indexes
import org.bson.Document
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import javax.xml.parsers.SAXParserFactory

@Deprecated("Use GAR parser")
class FiasParser : Harvester() {
    private val regionInt = regionCode.toInt()
    private val aotCollection = db.getCollection("temp_AddressObjectTypes")
    private val objectsCollection = db.getCollection("temp_AddrObjects")
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd")
    private var addrObjGuids = hashSetOf<String>()

    override fun mainHarvest(): Int {
        try {
            insertMetadata(datasetStructure)
            addrObjGuids.clear()
            aotCollection.drop()
            objectsCollection.drop()
            housesCollection.drop()
            val factory = SAXParserFactory.newInstance()
            val parser = factory.newSAXParser()

            val dir = settings.getJSONObject("options").getString("FiasCatalog")
            Handler().apply {
                parser.parse(File(dir + settings.getJSONObject("options").getString("FileSocrBase")), this)
                aotCollection.createIndex(Indexes.ascending("LEVEL"))
                aotCollection.createIndex(Indexes.ascending("SCNAME"))
                aotCollection.createIndex(Indexes.ascending("LEVEL", "SCNAME"))
                println("SOCRBASE processed")
                writeProgress(0.05, 0)
                parser.parse(File(dir + settings.getJSONObject("options").getString("FileAddrObject")), this)
                println("ADDROBJ processed")
                writeProgress(0.2, 0)

                objectsCollection.createIndex(Indexes.ascending("AOGUID"))

                parser.parse(File(dir + settings.getJSONObject("options").getString("FileHouses")), this)
                println("HOUSES processed")

                aotCollection.drop()
                objectsCollection.drop()

                housesCollection.createIndex(Indexes.ascending("ID"))
                housesCollection.createIndex(Indexes.ascending("RegionID"))
                housesCollection.createIndex(Indexes.ascending("MunicipalitetID"))
                housesCollection.createIndex(Indexes.ascending("StreetID"))
                housesCollection.createIndex(Indexes.ascending("BlockID"))
                housesCollection.createIndex(
                    Indexes.compoundIndex(
                        Indexes.text("Region"),
                        Indexes.text("Municipalitet"),
                        Indexes.text("Street"),
                        Indexes.text("HouseNumber")
                    )
                )
            }
        } catch (e: Exception) {
            writeError(e.message ?: "")
        }
        return housesCount
    }


    var housesCount = 0
    var addrObjCount = 0
    val date = Date()

    inner class Handler : DefaultHandler() {
        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            try {
                val document = Document()
                when (qName) {
                    "AddressObjectType" -> {
                        for (aotField in aotFields) {
                            document.append(aotField, attributes.getValue(aotField))
                        }
                        aotCollection.insertOne(document)
                    }

                    "Object" -> {
                        addrObjCount++
                        if (addrObjCount % 10000 == 0) println("$addrObjCount ADDROBJ processed")
                        if (attributes.getValue("ACTSTATUS") != "1") return
                        val rCode = attributes.getValue("REGIONCODE")
                        if (rCode != regionCode) return
                        for (objectField in objectFields) {
                            document.append(objectField, attributes.getValue(objectField))
                        }
                        objectsCollection.insertOne(document)
                        addrObjGuids.add(attributes.getValue("AOGUID"))
                    }

                    "House" -> {

                        val ifnsfl = attributes.getValue("IFNSFL")?.toIntOrNull() ?: return
                        if (ifnsfl < regionInt * 100 || ifnsfl > regionInt * 100 + 99) return
                        val endDate = dateFormat.parse(attributes.getValue("ENDDATE"))
                        if (endDate.before(date)) return
                        val aoGuid = attributes.getValue("AOGUID")
                        if (!addrObjGuids.contains(aoGuid)) return

                        val streets = objectsCollection.find(eq("AOGUID", aoGuid))
                        var streetsCount = 0
                        var resultStreet: Document? = null
                        for (street in streets) {
                            streetsCount++
                            resultStreet = street
                        }
                        if (streetsCount == 0) println("no street found")
                        if (streetsCount > 1) println("many streets found")

                        if (resultStreet == null) return
                        val resultMunicipality: Document?
                        when (resultStreet.getString("AOLEVEL")) {
                            "3", "4", "5", "6", "90", "91" -> {
                                resultMunicipality = resultStreet
                                resultStreet = null
                            }

                            else -> {
                                val municipalities = objectsCollection.find(
                                    eq("AOGUID", resultStreet.getString("PARENTGUID"))
                                )

                                resultMunicipality = municipalities.first() ?: return

                                when (resultMunicipality.getString("AOLEVEL")) {
                                    "3", "4", "5", "6", "90", "91" -> {
                                    }

                                    else -> return
                                }
                            }
                        }


                        var municipalityName = ""
                        var parentGuid = resultMunicipality.getString("PARENTGUID")
                        var region: Document? = null
                        var regionName = ""
                        var solid: Boolean? = null
//                        var blockID = parentGuid
                        while (parentGuid != null) {
                            val items = objectsCollection.find(
                                eq("AOGUID", parentGuid)
                            )
                            parentGuid = null
                            for (item in items) {
                                val types = aotCollection.find(
                                    Document("LEVEL", item.getString("AOLEVEL")).append(
                                        "SCNAME",
                                        item.getString("SHORTNAME")
                                    )
                                )
                                var resultType: String? = null
                                for (type in types) {
                                    resultType = type.getString("SOCRNAME")
                                }
                                if (resultType == null) return
                                regionName = "$resultType ${item.getString("FORMALNAME")}"
                                region = item
//                                val currentGuid = parentGuid
                                parentGuid = item.getString("PARENTGUID")
                                if (solid == null) solid = item.getString("AOLEVEL") == "1"
                                if (parentGuid != null) {
                                    municipalityName = "$regionName, $municipalityName"
//                                    blockID = currentGuid
                                }
                            }
                        }

                        if (region == null) return

                        var types = aotCollection.find(
                            Document("LEVEL", resultMunicipality.getString("AOLEVEL"))
                                .append("SCNAME", resultMunicipality.getString("SHORTNAME"))
                        )
                        var resultType: String? = types.first()?.getString("SOCRNAME") ?: return
                        municipalityName += "$resultType ${resultMunicipality.getString("FORMALNAME")}"

                        resultType = null
                        val streetName = resultStreet?.let {
                            types = aotCollection.find(
                                Document("LEVEL", resultStreet.getString("AOLEVEL"))
                                    .append("SCNAME", resultStreet.getString("SHORTNAME"))
                            )
                            resultType = types.first()?.getString("SOCRNAME") ?: return@let ""
                            "$resultType ${resultStreet.getString("FORMALNAME")}"
                        } ?: ""

//                println(resultName)
                        val regionID = region.getString("AOGUID")
                        val munID = resultMunicipality.getString("AOGUID")
                        var blockID = resultMunicipality.getString("PARENTGUID").let {
                            if (it == regionID) munID else it
                        }

                        var blockParent = blockID
                        while (blockParent != regionID) {
                            blockID = blockParent
                            blockParent = objectsCollection.find(
                                eq("AOGUID", blockParent)
                            ).first()?.getString("PARENTGUID")
                        }

                        document.append("ID", attributes.getValue("HOUSEGUID"))
                            .append("Region", regionName)
                            .append("RegionID", regionID)
                            .append("Municipalitet", municipalityName)
                            .append("MunicipalitetID", munID)
                            .append("BlockID", blockID)
                            .append("Solid", if (solid == true) 1 else 0)
                            .append("Street", streetName)
                            .append("StreetID", resultStreet?.getString("AOGUID") ?: "")
                            .append(
                                "HouseNumber",
                                (
                                        (attributes.getValue("HOUSENUM") ?: "") + " " +
                                                (attributes.getValue("STRUCNUM") ?: "") + " " +
                                                (attributes.getValue("BUILDNUM") ?: "")
                                        ).trim()
                            )
                            .append("PostalCode", attributes.getValue("POSTALCODE"))
                            .append("x", null)
                            .append("y", null)
                            .append("Geometry", Document("type", "WTF").append("coordinates", null))

                        housesCollection.insertOne(document)

                        housesCount++
                        if (housesCount % 10000 == 0) {
                            println("$housesCount HOUSES processed")
                            var progress = housesCount / 500000.0 * 0.8 + 0.2
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
        val aotFields = listOf("LEVEL", "SOCRNAME", "SCNAME", "KOD_T_ST")
        val objectFields = listOf(
            "AOID", "AOGUID", "PARENTGUID", "FORMALNAME", "OFFNAME", "SHORTNAME", "AOLEVEL", "REGIONCODE",
            "AREACODE", "AUTOCODE", "CITYCODE", "CTARCODE", "PLACECODE", "STREETCODE", "EXTRCODE", "SEXTCODE",
            "PLAINCODE", "CODE", "CURRSTATUS", "ACTSTATUS", "LIVESTATUS", "CENTSTATUS", "OPERSTATUS", "IFNSFL",
            "IFNSUL", "TERRIFNSFL", "TERRIFNSUL", "OKATO", "OKTMO", "POSTALCODE", "STARTDATE", "ENDDATE",
            "UPDATEDATE", "NEXTID", "PREVID", "NORMDOC"
        )
        val houseFields = listOf(
            "HOUSEGUID", "HOUSEID", "HOUSENUM", "STRSTATUS", "ESTSTATUS", "STATSTATUS", "IFNSUL", "COUNTER",
            "NORMDOC", "STRUCNUM", "BUILDNUM", "TERRIFNSFL", "TERRIFNSUL", "OKATO", "OKTMO", "POSTALCODE",
            "STARTDATE", "ENDDATE", "UPDATEDATE"
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