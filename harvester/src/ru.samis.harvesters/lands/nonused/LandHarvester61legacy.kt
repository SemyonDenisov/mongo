package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester61legacy : LandHarvester() {

    override fun parseSemantics(): Int {
        val fileNames = params.getJSONObject("params").getString("csv").split(",")
        val dir = params.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        for (fileName in fileNames) {
            println(fileName)
            FileReader(dir + fileName).buffered().use { reader ->
                var line = reader.readLine()
                while (line.split("^").last().toIntOrNull() == null)
                    line = reader.readLine()

                while (line != null) {
                    val tokens = line.split("^").toMutableList()
                    if (tokens.size < 4)
                        System.err.println(tokens)
                    else {
                        val cadNumInt = tokens[1]
                            .replace(":", "")
                            .replace("(", "")
                            .replace(")", "")
                            .toLongOrNull()

                        cadNumInt?.let {
                            if (housesCollection.updateOne(
                                    Filters.eq("cadNumInt", cadNumInt),
                                    Updates.combine(
//                                        Updates.set("BlockID", null),
                                        Updates.set("Municipalitet", null),
                                        Updates.set("MunicipalitetID", null),
                                        Updates.set("Street", null),
                                        Updates.set("StreetID", null),
                                        Updates.set("HouseNumber", null),
                                        Updates.set("Date", null),
                                        Updates.set("Square", null),
                                        Updates.set("PostalCode", null),
                                        Updates.set("AddressDesc", tokens[2]),
                                        Updates.set("UsageCode", null),
                                        Updates.set("AllowedCode", null),
                                        Updates.set("AllowedClass", null),
                                        Updates.set("AllowedDoc", null),
                                        Updates.set("Allowed", tokens[3]),
                                        Updates.set("CadastrCost", null),
                                        Updates.set("CadastrCostEstDate", null),
                                        Updates.setOnInsert(
                                            Document("cadNum", tokens[1])
                                                .append("Geometry", Document("type", "WTF").append("coordinates", null))
                                                .append("Region", regionName)
                                                .append("RegionID", regionID)
                                                .append("RegionCode", regionCode)
                                        )
                                    ),
                                    UpdateOptions().upsert(true)
                                ).upsertedId != null
                            ) {
                                inserted++
                                incCount()
                            }
                            updated++
                            if (updated % 10000 == 0) {
                                println("$updated updated")
//                        var progress = inserted / 500000.0
//                        if (progress > 0.99) progress = 0.99
//                        writeProgress(progress, inserted)
                            }
                            true
                        } ?: System.err.println(tokens)
                    }

                    line = reader.readLine()
                }
            }
        }

        return inserted
    }
}