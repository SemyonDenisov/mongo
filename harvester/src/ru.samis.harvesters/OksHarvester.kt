package ru.samis.harvesters
/*
import com.mongodb.client.model.Indexes
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import java.sql.DriverManager
import java.sql.Statement

class OksHarvester : Harvester() {
    private var postgreConnString = ""

    override fun mainHarvest():Int {

        postgreConnString = params.getString("postgreConnString")

        return withPostgre { st ->
            st.executeQuery(QUERY).use { resultSet ->
                housesCollection.apply {
                    drop()

                    createIndex(Indexes.ascending("ID"))
                    createIndex(Indexes.ascending("RegionID"))
                    createIndex(Indexes.ascending("MunicipalitetID"))
                    createIndex(Indexes.ascending("StreetID"))
                    createIndex(Indexes.ascending("BlockID"))
                    createIndex(Indexes.ascending("cadNumInt"))
                    createIndex(Indexes.ascending("cadNum"))
                    createIndex(Indexes.ascending("Region"))
                    createIndex(Indexes.ascending("Municipalitet"))
                    createIndex(Indexes.ascending("Street"))
                    createIndex(Indexes.ascending("HouseNumber"))

                    createIndex(
                        Indexes.compoundIndex(
                            Indexes.text("Region"),
                            Indexes.text("Municipalitet"),
                            Indexes.text("Street"),
                            Indexes.text("HouseNumber")
                        )
                    )
                }
                val regionName = params.getString("region")
                val regionID = DigestUtils.sha1Hex(regionName)

                var count = 0

                while (resultSet.next()) {
                    val doc = Document()
                        .append("Region", regionName)
                        .append("RegionID", regionID)
                        .append("RegionCode", regionCode)
                        .append("Geometry", Document("type", "WTF").append("value", null))
                        .append("Height", resultSet.getString(resultSet.findColumn("height")))
                        .append("OKTMO", resultSet.getString(resultSet.findColumn("oktmo")))
                        .append("Cladr", resultSet.getString(resultSet.findColumn("kladr")))
                        .append("OKATO", resultSet.getString(resultSet.findColumn("okato")))
                        .append("PostalCode", resultSet.getString(resultSet.findColumn("postal_code")))
                        .append("AddressDesc", resultSet.getString(resultSet.findColumn("location")))
                        .append("BuildingType", resultSet.getString(resultSet.findColumn("oks_type")))
                        .append("Date", resultSet.getString(resultSet.findColumn("date_create")))
                        .append("Square", resultSet.getDouble(resultSet.findColumn("area")))

                    try {
                        doc.append(
                            "Geometry", Document.parse(
                                resultSet.getString(resultSet.findColumn("st_asgeojson"))
                            )
                        )
                    } catch (e: Exception) {
//                        println(resultSet.getString(resultSet.findColumn("st_asgeojson")))
                    }

                    var token = resultSet.getString(resultSet.findColumn("cadastral_num"))
                    val cadNumInt = (token.toString()
                        .replace(":", "")
                        .replace("(", "")
                        .replace(")", "")
                            ).toLongOrNull() ?: 0
                    val cadDigits = token.toString().split(":")
                    doc.append("cadNum", token)
                        .append(
                            "BlockID",
                            if (cadDigits.size >= 2) cadDigits.subList(0, 2).joinToString(":") else token
                        )
                        .append("cadNumInt", cadNumInt)
                        .append("ID", token)

                    var district = ""
                    var city = ""
                    var locality = ""
                    var street = ""
                    var house = ""
                    var level3 = ""
                    var level2 = ""

                    val metaData = resultSet.metaData
                    for (i in 1..metaData.columnCount) {
                        val caption = metaData.getColumnLabel(i)
                        val token = resultSet.getObject(i)
//                        val token = when (token) {
//                            "NULL" -> null
//                            "TRUE" -> true
//                            "FALSE" -> false
//                            else -> token
//                        }

                        when (caption) {
                            "district_name" -> district = "$district ${token?.toString() ?: ""}"
                            "district_type" -> district = "${token?.toString() ?: ""} $district"
                            "city_name" -> city = "$city ${token?.toString() ?: ""}"
                            "city_type" -> city = "${token?.toString() ?: ""} $city"
                            "locality_name" -> locality = "$locality ${token?.toString() ?: ""}"
                            "locality_type" -> locality = "${token?.toString() ?: ""} $locality"
                            "street_name" -> street = "$street ${token?.toString() ?: ""}"
                            "street_type" -> street = "${token?.toString() ?: ""} $street"
                            "level1_value" -> house = "$house ${token?.toString() ?: ""}"
                            "level1_type" -> house = "${token?.toString() ?: ""} $house"
                            "level2_value" -> level2 = "$level2 ${token?.toString() ?: ""}"
                            "level2_type" -> level2 = "${token?.toString() ?: ""} $level2"
                            "level3_value" -> level3 = "$level3 ${token?.toString() ?: ""}"
                            "level3_type" -> level3 = "${token?.toString() ?: ""} $level3"

                            "floors", "underground_floors", "material", "year_built", "year_use", "special_note", "parts_info", "cultural_objects_description",
                            "soviet_village_name", "urban_district_name", "fias" -> doc.append(caption, token)

                            "address_id" -> doc.append(caption, token?.toString())
                        }
                    }

                    city = (city + locality).replace("  ", " ")
                    street = street.replace("  ", " ")
                    house = house.replace("  ", " ")
                    level2 = level2.replace("  ", " ")
                    level3 = level3.replace("  ", " ")

                    doc.append("Municipalitet", city.trim())
                        .append("MunicipalitetID", DigestUtils.sha1Hex(city))
                        .append("Street", street.trim())
                        .append("StreetID", DigestUtils.sha1Hex(city))
                        .append("HouseNumber", house.trim())
                        .append("level2", level2.trim())
                        .append("level3", level3.trim())

                    try {
//                        println(doc.toJson())
                        housesCollection.insertOne(doc)
                    } catch (e: Exception) {
                        e.printStackTrace()
                        writeError("doc: $doc\nexception: ${e.localizedMessage}")
                    }
                    count++

                    if (count % 10000 == 0)
                        println(count)
                    writeProgress(count / 713200.0, 713200)
                }

                return@use count
            }
        }


    }

    private fun parseLine(line: String): MutableList<String> {
        var depth = 0
        var startIndex = 0
        val tokens = mutableListOf<String>()
        for ((i, c) in line.withIndex()) {
            when (c) {
                '"' -> {
                    depth = 1 - depth
                }

                ',' -> {
                    if (depth == 0) {
                        var token = line.substring(startIndex, i)
                        if (token[0] == '"') token = token.substring(1 until token.length - 1)
                        tokens += token
                        startIndex = i + 1
                    }
                }
            }
        }

        return tokens
    }

    fun <T> withPostgre(callback: (Statement) -> T): T {
        Class.forName("org.postgresql.Driver")

        return DriverManager.getConnection(postgreConnString).use {
            it.createStatement().use(callback)
        }
    }

    companion object {
        val QUERY =
            "SELECT o.id, status, purpose, cadastral_num, \"location\", \"name\", ST_AsGeoJson(ST_Transform(geom, 4326)), \n" +
                    "  oks_type, registration_date, cad_quarter, parent_landplot_cad_number, \n" +
                    "  area, floors, underground_floors, height, sito_info, cadastral_cost, \n" +
                    "  material, year_built, year_use,   \n" +
                    "  special_note, parts_info, cultural_objects_description,  \n" +
                    "  o.date_update, is_geometry_official, address_id,\n" +
                    "  a.*\n" +
                    "FROM isogd.oks o\n" +
                    "left join isogd.address a on (a.id=o.address_id)\n" +
                    "where coalesce(record_status, '06')='06'"
    }
}
*/