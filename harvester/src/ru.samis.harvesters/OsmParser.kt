package ru.samis.harvesters

import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.util.*
import javax.xml.parsers.SAXParserFactory

@Deprecated("Use JSON version", ReplaceWith("OsmParserJson"))
class OsmParser(
    private val file: String
) : FragmentationHarvester() {


    override fun mainHarvest(): Int {
        housesCollection.drop()
        buffer.clear()
        val factory = SAXParserFactory.newInstance()
        val parser = factory.newSAXParser()

        parser.parse(File(file), Handler())
        housesCollection.insertMany(buffer)

        return 0
    }


    class LatLon(val lat: Double, val lon: Double)

    private var wayId = ""
    private var wayUid = ""
    private val nodes = hashMapOf<String, LatLon>()
    private val wayNodes = arrayListOf<String>()
    private var wayTagAddrCity = ""
    private var wayTagAddrHousenumber = ""
    private var wayTagAddrPostcode = ""
    private var wayTagAddrStreet = ""
    private var wayTagBuilding = ""
    private var wayTagHighway = ""
    private var wayTagName = ""
    private var wayTagSurface = ""
    private var wayTagMaxspeed = ""
    private var wayTagRailway = ""
    private var wayTagOneway = ""
    private var wayTagLanes = ""

    private val buffer = mutableListOf<Document>()

    inner class Handler : DefaultHandler() {
        @Throws(SAXException::class)
        override fun endElement(uri: String, localName: String, qName: String) {
            super.endElement(uri, localName, qName)
            if (qName.uppercase(Locale.getDefault()) != "WAY") return
            if (wayTagBuilding.isEmpty()) return

            var lat = 0.0
            var lon = 0.0
            val geometry = List(1) {
                List(wayNodes.size) { i ->
                    val latLon = nodes[wayNodes[i]]
                    latLon?.let {
                        lat += latLon.lat
                        lon += latLon.lon
                        List(2) { i -> if (i == 0) latLon.lon else latLon.lat }
                    } ?: List(2) { 0.0 }
                }
            }
            lat /= wayNodes.size
            lon /= wayNodes.size
            if (!isInRegion(lat, lon)) return
            var districtName = ""
            nodes[wayNodes[0]]?.apply {
                districtName = findArea(lat, lon)
//            println(districtName)
            }


            val document = Document("id", wayId)
                .append("Region", "Самарская область")
                .append("RegionID", DigestUtils.sha1Hex("Самарская область"))
                .append("Municipalitet", wayTagAddrCity)
                .append("MunicipalitetID", DigestUtils.sha1Hex(wayTagAddrCity))

                .append("BlockID", districtName)

                .append("Solid", 1)
                .append("Street", wayTagAddrStreet)
                .append("StreetID", DigestUtils.sha1Hex(wayTagAddrStreet))
                .append("HouseNumber", wayTagAddrHousenumber)
                .append("PostalCode", wayTagAddrPostcode)
                .append("Geometry", Document("type", "Polygon").append("coordinates", geometry))
                .append("x", lat)
                .append("y", lon)

//        println(document)
            buffer += document
            if (buffer.size == 10) {
                housesCollection.insertMany(buffer)
                buffer.clear()
            }
        }

        @Throws(SAXException::class)
        override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
            when (qName.uppercase(Locale.getDefault())) {
                "NODE" -> {
                    nodes[attributes.getValue("id")] = LatLon(
                        attributes.getValue("lat").toDouble(),
                        attributes.getValue("lon").toDouble()
                    )
                }

                "WAY" -> {
                    wayId = attributes.getValue("id")
                    wayUid = attributes.getValue("uid")
                    wayTagAddrCity = ""
                    wayTagAddrHousenumber = ""
                    wayTagAddrPostcode = ""
                    wayTagAddrStreet = ""
                    wayTagBuilding = ""
                    wayTagHighway = ""
                    wayTagName = ""
                    wayTagSurface = ""
                    wayTagMaxspeed = ""
                    wayTagRailway = ""
                    wayTagOneway = ""
                    wayTagLanes = ""
                    wayNodes.clear()
                }

                "ND" -> {
                    wayNodes += attributes.getValue("ref")
                }

                "TAG" -> {
                    val value = attributes.getValue("v")
                    when (attributes.getValue("k")) {
                        "addr:city" -> wayTagAddrCity = value
                        "addr:housenumber" -> wayTagAddrHousenumber = value
                        "addr:postcode" -> wayTagAddrPostcode = value
                        "addr:street" -> wayTagAddrStreet = value
                        "building" -> wayTagBuilding = value
                        "highway" -> wayTagHighway = value
                        "name" -> wayTagName = value
                        "surface" -> wayTagSurface = value
                        "maxspeed" -> wayTagMaxspeed = value
                        "railway" -> wayTagRailway = value
                        "oneway" -> wayTagOneway = value
                        "lanes" -> wayTagLanes = value
                    }
                }
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
    }

}