package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester66legacy : LandHarvester() {
    override fun parseSemantics(): Int {
        val fileNames = params.getJSONObject("params").getString("csv").split(",")
        val dir = params.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        val inserter = { tokens: List<String> ->
            val cadNumInt = tokens[1]
                .replace(":", "")
                .replace("(", "")
                .replace(")", "")
                .toLongOrNull()

            cadNumInt?.let {
                if (housesCollection.updateOne(
                        Filters.eq("cadNumInt", cadNumInt),
                        Updates.combine(
//                            Updates.set("BlockID", concat(tokens, 20, 21)),
                            Updates.set("Municipalitet", concat(tokens, 22, 29)),
                            Updates.set("Street", concat(tokens, 30, 31)),
                            Updates.set("HouseNumber", concat(tokens, 32, 37)),
                            Updates.set("Flat", concat(tokens, 38, 39)),
                            Updates.set("Date", tokens[2]),
                            Updates.set("Square", tokens[6]),
                            Updates.set("PostalCode", tokens[17]),
                            Updates.set("AddressDesc", tokens[41]),
                            Updates.set("AllowedCode", tokens[59]),
                            Updates.set("AllowedClass", tokens[43]),
                            Updates.set("AllowedDoc", tokens[44]),
                            Updates.set("Allowed", tokens[45]),
                            Updates.set("CadastrCost", tokens[46]),
                            Updates.set("CadastrCostEstDate", tokens[48]),
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

        for (fileName in fileNames) {
            FileReader(dir + fileName).buffered().use { reader ->
                reader.readLine()
                reader.readLine()

                var line = reader.readLine()
                var oldLine: String? = null
                while (line != null) {
                    var tokens = line.split("^").toMutableList()
                    oldLine?.let {
                        if (tokens.size > 1 && tokens[0].toIntOrNull() != null && tokens[1].contains(":")) {
                            val newTokens = it.split("^").toMutableList()

                            while (newTokens.size < 60) newTokens.add("")
                            inserter(newTokens)
                            oldLine = null
                        } else {
                            oldLine += " $line"
                            tokens = oldLine!!.split("^").toMutableList()
                        }
                    }

                    if (tokens.size < 60) {
                        System.err.println(tokens)
                        oldLine = if (oldLine == null) line else "$oldLine $line"
                        line = reader.readLine()
                        continue
                    }
                    oldLine = null
                    inserter(tokens)
                    line = reader.readLine()
                }
            }
        }

        return inserted
    }
}