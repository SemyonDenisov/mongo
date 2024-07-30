package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester24legacy : LandHarvester() {
    init {
        cadNumKey = "LABEL"
    }

    override fun parseSemantics(): Int {
        val fileNames = params.getJSONObject("params").getString("csv").split(",")
        val dir = params.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        for (fileName in fileNames) {
            println(fileName)
            FileReader(dir + fileName).buffered().use { reader ->
                reader.readLine()
                reader.readLine()
                reader.readLine()
                reader.readLine()
                reader.readLine()
                reader.readLine()

                var line = reader.readLine()

                while (line != null) {
                    var tokens = line.split("^").toMutableList()
                    while (tokens.size < 63) {
                        val newLine = reader.readLine() ?: break
                        line += "\n" + newLine
                        tokens = line.split("^").toMutableList()
                    }
                    if (tokens.size < 63) break
//                    println(tokens)

                    val cadNumInt = tokens[1]
                        .replace(":", "")
                        .replace("(", "")
                        .replace(")", "")
                        .toLongOrNull()

                    cadNumInt?.let {
                        val mun = concat(tokens, 38, 47)
                        val street = concat(tokens, 48, 49)
                        inserted += if (housesCollection.updateOne(
                                Filters.eq("cadNumInt", cadNumInt),
                                Updates.combine(
//                                    Updates.set("BlockID", concat(tokens, 36, 37)),
                                    Updates.set("Municipalitet", mun),
                                    Updates.set("MunicipalitetID", DigestUtils.sha1Hex(mun)),
                                    Updates.set("Street", street),
                                    Updates.set("StreetID", DigestUtils.sha1Hex(street)),
                                    Updates.set("HouseNumber", concat(tokens, 50, 55)),
                                    Updates.set("Flat", concat(tokens, 56, 57)),
                                    Updates.set("Date", tokens[18]),
                                    Updates.set("Square", tokens[10]),
                                    Updates.set("PostalCode", tokens[35]),
                                    Updates.set("AddressDesc", concat(tokens, 58, 59)),
                                    Updates.set("AllowedCode", tokens[4]),
                                    Updates.set("AllowedClass", tokens[13]),
                                    Updates.set("AllowedDoc", tokens[8]),
                                    Updates.set("Allowed", tokens[16]),
                                    Updates.set("CadastrCost", tokens[20]),
                                    Updates.set("CadastrCostEstDate", tokens[21]),
                                    Updates.set("FiasCode", tokens[33]),
                                    Updates.setOnInsert(
                                        Document("cadNum", tokens[1])
                                            .append("Geometry", Document("type", "WTF").append("coordinates", null))
                                            .append("Region", regionName)
                                            .append("RegionID", regionID)
                                            .append("RegionCode", regionCode)
                                    )
                                ),
                                UpdateOptions().upsert(true)
                            ).upsertedId == null
                        ) 0 else {
                            incCount()
                            1
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


                    line = reader.readLine()
                }
            }
        }
        return inserted
    }
}