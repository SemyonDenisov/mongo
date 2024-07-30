package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester59legacy : LandHarvester() {

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

                var line = reader.readLine()

                while (line != null) {
                    var tokens = line.split("^").toMutableList()

                    val cadNumInt = tokens[1]
                        .replace(":", "")
                        .replace("(", "")
                        .replace(")", "")
                        .toLongOrNull()

                    /*№ п/п^
                    Кадастровый номер земельного участка^
                    Кадастровый квартал^
                    Площадь земельного участка, кв.м.^
                    Вид разрешенного использования^
                    Вид разрешенного использования по документу^
                    Неформализованное описание^
                    Муниципальное образование^
                    Населенный пункт^
                    Кадастровая стоимость, руб.^
                    УПКС, руб.кв.м.^
                    Дата присвоения кадастрового номера^
                    Кадастровые номера ОН, расположенных в пределах земельного участка и группа^
                    Код ФИАС (КЛАДР) муниципального района^
                    Код ФИАС (КЛАДР) населенного пункта^
                    ОКТМО муниципального района^
                    Сегмент (в соответствии с приложением 1 к Методическим указаниям)^
                    Код вида использования  (в соответствии с приложением 1 к Методическим указаниям)^
                    Код вида разрешенного использования (в соответствии с классификатором видов разрешенного использования земельных участков)^
                    Источник информации о виде использования объектов недвижимости^
                    Сведения о нахождении на земельном участке других связанных с ним объектов недвижимости^
                    Источник информации о нахождении на земельном участке других связанных с ним объектов недвижимости^
                    Достаточность характеристик для опеделения кадастровой стоимости^^^^^*/

                    cadNumInt?.let {
                        inserted += if (housesCollection.updateOne(
                                Filters.eq("cadNumInt", cadNumInt),
                                Updates.combine(
//                                    Updates.set("BlockID", tokens[7]),
                                    Updates.set("Municipalitet", tokens[8]),
                                    Updates.set("MunicipalitetID", DigestUtils.sha1Hex(tokens[8])),
                                    Updates.set("Street", null),
                                    Updates.set("StreetID", null),
                                    Updates.set("HouseNumber", null),
                                    Updates.set("Date", tokens[11]),
                                    Updates.set("Square", tokens[3]),
                                    Updates.set("PostalCode", null),
                                    Updates.set("BlockOKTMO", tokens[15]),
                                    Updates.set("BlockCladr", tokens[13]),
                                    Updates.set("MunicipalitetCladr", tokens[14]),
                                    Updates.set("AddressDesc", tokens[6]),
                                    Updates.set("UsageCode", tokens[17]),
                                    Updates.set("AllowedCode", tokens[18]),
                                    Updates.set("AllowedClass", null),
                                    Updates.set("AllowedDoc", tokens[5]),
                                    Updates.set("Allowed", tokens[4]),
                                    Updates.set("CadastrCostEstDate", null),
                                    Updates.set("InnerObjects", tokens[20]),
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