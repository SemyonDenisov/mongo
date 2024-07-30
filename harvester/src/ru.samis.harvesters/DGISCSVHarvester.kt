package ru.samis.harvesters

import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.json.JSONObject
import java.io.File
import java.io.FileReader
import kotlin.math.ln
import kotlin.math.sin

class DGISCSVHarvester : FragmentationHarvester() {

    override fun mainHarvest(): Int {
        insertMetadata(datasetStructure)

        housesCollection.drop()
        val dir = settings.getJSONObject("options").getString("2gisCatalog")

        var readCount = 0

        var count = 0
        val files = params.optString("regionFiles").split(";")

        val orgFiles = params.optString("orgFiles").split(";")

        orgFiles.forEach { fileName ->
            val file = File(dir + fileName)
            if (!file.exists() || file.isDirectory) return@forEach
            println("enumerating $file")
            FileReader(file).buffered().apply {
                readLine()
                while (true) {
                    readLine() ?: return@apply
                    count++
                }
            }
        }

        files.forEach { fileName ->
            val file = File(dir + fileName)
            if (!file.exists() || file.isDirectory) return@forEach
            FileReader(file).buffered().apply {
                readLine()
                while (true) {
                    readLine() ?: return@apply
                    count++
                }
            }
        }

        files.forEach { fileName ->
            val file = File(dir + fileName)
            if (!file.exists() || file.isDirectory) return@forEach
            FileReader(file).buffered().apply {
                readLine()

                while (true) {
                    try {
                        val tokens = readLine()?.split(";") ?: return@apply
//                    println(tokens)
                        val coords = listOf(
                            tokens[4].replace(",", ".").toDouble(),
                            tokens[5].replace(",", ".").toDouble()
                        )
                        val districtName = findArea(coords[1], coords[0])

                        housesCollection.insertOne(
                            Document()
//                                .append("ID", DigestUtils.sha1Hex(region + tokens[0] + tokens[2] + tokens[3]))
                                .append("ID", "${coords[0]}${coords[1]}")
                                .append("Region", region)
                                .append("RegionID", DigestUtils.sha1Hex(region))
                                .append("Municipalitet", tokens[0])
                                .append("MunicipalitetID", DigestUtils.sha1Hex(region + tokens[0]))
//                                .append("BlockID", resultSet.getInt(blockIdIndex))
                                .append("BlockID", districtName)
                                .append("Solid", 1)
                                .append("Street", tokens[2])
                                .append("StreetID", DigestUtils.sha1Hex(region + tokens[0] + tokens[2]))
                                .append("HouseNumber", tokens[3])
                                .append("BuildingType", tokens[6])
                                .append("PostalCode", tokens[8])
                                .append("Levels", tokens[7])
                                .append("RegionCode", regionCode)
                                .append(
                                    "y",
                                    3189068.5 * ln((1.0 + sin(coords[1] * 0.017453292519943295)) / (1.0 - sin(coords[1] * 0.017453292519943295)))
                                )
                                .append("x", coords[0] * 0.017453292519943295 * 6378137.0)
                                .append(
                                    "Geometry",
                                    Document()
                                        .append("type", "Point")
                                        .append(
                                            "coordinates",
                                            coords
                                        )
                                )
                        )
                        readCount++
                        if (readCount % 10000 == 0) {
                            val progress = 1.0 * readCount / count
                            println(progress)
                            writeProgress(progress, readCount)
                        }
                    } catch (e: Exception) {
                        writeError(e.localizedMessage)
                        e.printStackTrace()
                    }
                }
            }
        }

        housesCollection.apply {
            createIndex(Indexes.ascending("x"))
            createIndex(Indexes.ascending("y"))
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("MunicipalitetID"))
            createIndex(Indexes.ascending("Municipalitet"))
            createIndex(Indexes.ascending("StreetID"))
            createIndex(Indexes.ascending("Street"))
            createIndex(Indexes.ascending("HouseNumber"))
            createIndex(Indexes.ascending("BlockID"))
            createIndex(
                Indexes.compoundIndex(
                    Indexes.text("Region"),
                    Indexes.text("Municipalitet"),
                    Indexes.text("Street"),
                    Indexes.text("HouseNumber")
                )
            )
            createIndex(
                Indexes.compoundIndex(
                    Indexes.ascending("Municipalitet"),
                    Indexes.ascending("Street"),
                    Indexes.ascending("HouseNumber")
                )
            )
        }

        var orgCount = 0
        for (fileName in orgFiles) {
            val file = File(dir + fileName)
            if (!file.exists() || file.isDirectory) continue

            FileReader(file).buffered().apply {
                while (true) {
                    try {
                        val json = readLine() ?: return@apply
                        val obj = JSONObject(json)
                        val fils = obj.getJSONArray("fils")
                        for (i in 0 until fils.length()) {
                            val fil = fils.getJSONObject(i)
                            val addresses = fil.getJSONArray("addresses")
                            for (j in 0 until addresses.length()) {
                                val address = addresses.getJSONObject(j)
                                val street = address.getString("street")
                                if (street.isBlank()) continue
                                val house = address.getString("house")
                                if (house.isBlank()) continue
                                val city = address.getString("city")
                                if (city.isBlank()) continue

                                val houses = housesCollection.find(
                                    and(
                                        eq("Municipalitet", city),
                                        eq("Street", street),
                                        eq("HouseNumber", house)
                                    )
                                )

                                for (document in houses) {

                                    val orgs = (document["orgs"] as MutableList<Document>?) ?: arrayListOf()
                                    val id = obj["id"]
                                    val exists = orgs.find { it["id"] == id } != null

                                    if (exists) continue
                                    orgs += Document.parse(json)


                                    housesCollection.updateOne(
                                        eq("ID", document["ID"]),
                                        Updates.set("orgs", orgs),
                                        UpdateOptions().upsert(true)
                                    )

                                }
                            }
                        }

                        orgCount++
                        if (orgCount % 1000 == 0) {
                            println("$orgCount orgs")
                            writeProgress(1.0 * (readCount + orgCount) / count, readCount)
                        }
                    } catch (e: Exception) {
                        writeError(e.localizedMessage)
                        e.printStackTrace()
                    }
                }
            }
        }




        return readCount
    }


    companion object {
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
