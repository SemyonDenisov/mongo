package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester16legacy : LandHarvester() {
    override fun parseSemantics(): Int {
        val fileNames = params.getString("csv").split(",")
        val dir = settings.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        for (fileName in fileNames) {
            println(fileName)
            FileReader(dir + fileName).buffered().use { reader ->
                reader.readLine()
                reader.readLine()
                reader.readLine()
                reader.readLine()

                var line = reader.readLine()

                while (line != null) {
                    var tokens = line.split("^").toMutableList()
                    while (tokens.size < 80) {
                        val newLine = reader.readLine() ?: break
                        line += "\n" + newLine
                        tokens = line.split("^").toMutableList()
                    }
                    if (tokens.size < 80) break
//                    println(tokens)

                    val cadNumInt = tokens[1]
                        .replace(":", "")
                        .replace("(", "")
                        .replace(")", "")
                        .toLongOrNull()

                    cadNumInt?.let {
                        val mun = concat(tokens, 14, 19)
                        val street = concat(tokens, 20, 21)
                        inserted += if (housesCollection.updateOne(
                                Filters.eq("cadNumInt", cadNumInt),
                                Updates.combine(
//                                    Updates.set("BlockID", tokens[13]),
                                    Updates.set("Municipalitet", mun),
                                    Updates.set("MunicipalitetID", DigestUtils.sha1Hex(mun)),
                                    Updates.set("Street", street),
                                    Updates.set("StreetID", DigestUtils.sha1Hex(street)),
                                    Updates.set("HouseNumber", concat(tokens, 22, 27)),
                                    Updates.set("Flat", concat(tokens, 28, 29)),
                                    Updates.set("Date", tokens[2]),
                                    Updates.set("Square", tokens[5]),
                                    Updates.set("PostalCode", null),
                                    Updates.set("AddressDesc", concat(tokens, 30, 31)),
                                    Updates.set("AllowedCode", null),
                                    Updates.set("AllowedClass", tokens[33]),
                                    Updates.set("AllowedDoc", tokens[34]),
                                    Updates.set("Allowed", null),
                                    Updates.set("CadastrCost", tokens[41]),
                                    Updates.set("CadastrCostEstDate", tokens[43]),
                                    Updates.setOnInsert(
                                        Document("cadNum", tokens[1])
                                            .append("Geometry", Document("type", "WTF").append("coordinates", null))
                                            .append("Region", regionName)
                                            .append("RegionID", regionID)
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