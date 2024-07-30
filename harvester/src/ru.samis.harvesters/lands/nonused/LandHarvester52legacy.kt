package ru.samis.harvesters.lands.nonused

import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import ru.samis.harvesters.lands.LandHarvester
import java.io.FileReader

class LandHarvester52legacy : LandHarvester() {

    override fun parseSemantics(): Int {
        val fileNames = params.getString("csv").split(",")
        val dir = settings.getJSONObject("options").getString("catalog")
        var updated = 0
        var inserted = 0

        for (fileName in fileNames) {
            println(fileName)
            FileReader(dir + fileName).buffered().use { reader ->
                reader.readLine()

                var line = reader.readLine()

                while (line != null) {
                    var tokens = line.split("^").toMutableList()

                    val cadNum = tokens[0].replace(".", ":")
                    val cadNumInt = cadNum
                        .replace(":", "")
                        .replace("(", "")
                        .replace(")", "")
                        .toLongOrNull()

                    /*Кадастровый номер^
                    Кадастровый квартал^
                    Сегмент^
                    Код расчета вида использования^
                    Код ВРИ^
                    Кадастровые номера ОН, расположенных в пределах земельного участка^
                    Данные о едином землепользовании^
                    Вид использования по документам^
                    Наименование участка^
                    Площадь^
                    Сведения о правах^
                    Вид использования участка^
                    Вид разрешенного использования участка^
                    Вид использования объектов недвижимости^
                    Источник информации о виде использования объектов недвижимости^
                    Описание разрешенного использования^
                    Природные объекты на участке^
                    Дата постановки на учет^
                    Категория^
                    Дата определения кадастровой стоимости^
                    Дата внесения сведений о кадастровой стоимости в ЕГРН^
                    Дата утверждения кадастровой стоимости^
                    Номер акта об утверждении кадастровой стоимости^
                    Дата акта об утверждении кадастровой стоимости^
                    Наименование документа об утверждении кадастровой стоимости^
                    Дата начала применения кадастровой стоимости^
                    Дата подачи заявления о пересмотре кадастровой стоимости^
                    Дата последних изменений кадастровой стоимости^
                    ОКАТО^
                    КЛАДР^
                    ОКТМО^
                    ФИАС^
                    Код региона^
                    Почтовый индекс^
                    Тип района^
                    Название района^
                    Тип города^
                    Название города^
                    Тип городского района^
                    Название городского района^
                    Тип сельсовета^
                    Название сельсовета^
                    Тип населенного пункта^
                    Название населенного пункта^
                    Тип элемента планировочной структуры^
                    Название элемента планировочной структуры^
                    Тип улицы^
                    Название улицы^
                    Тип дома^
                    Номер дома^
                    Тип корпуса^
                    Название корпуса^
                    Тип строения^
                    Название строения^
                    Тип квартиры^
                    Номер квартиры^
                    Иное описание адреса^
                    Неформализованное описание^
                    Ограничения оборотоспособности участка^
                    Имущественный комплекс^
                    Реестровые номера границ зон или территорий*/

                    val mun = concat(tokens, 36, 43)

                    cadNumInt?.let {
                        val cadDigits = cadNum.split(":")
                        inserted += if (housesCollection.updateOne(
                                Filters.eq("cadNumInt", cadNumInt),
                                Updates.combine(
                                    Updates.set("MunDistrict", concat(tokens, 34, 35)),
                                    Updates.set("Municipalitet", mun),
                                    Updates.set("MunicipalitetID", DigestUtils.sha1Hex(mun)),
                                    Updates.set("Street", concat(tokens, 44, 47)),
                                    Updates.set("StreetID", DigestUtils.sha1Hex(concat(tokens, 44, 47))),
                                    Updates.set("HouseNumber", concat(tokens, 48, 53)),
                                    Updates.set("Flat", concat(tokens, 54, 55)),
                                    Updates.set("Date", tokens[17]),
                                    Updates.set("Square", tokens[9]),
                                    Updates.set("PostalCode", tokens[33]),
                                    Updates.set("AddressOther", tokens[56]),
                                    Updates.set("AddressDesc", tokens[57]),
                                    Updates.set("UsageCode", null),
                                    Updates.set("Usage", tokens[11]),
                                    Updates.set("OKATO", tokens[28]),
                                    Updates.set("Cladr", tokens[29]),
                                    Updates.set("OKTMO", tokens[30]),
                                    Updates.set("FIAS", tokens[31]),
                                    Updates.set("AllowedCode", tokens[4]),
                                    Updates.set("AllowedClass", null),
                                    Updates.set("AllowedDoc", tokens[7]),
                                    Updates.set("Allowed", tokens[12]),
                                    Updates.set("CadastrCost", tokens[9]),
                                    Updates.set("CadastrCostEstDate", tokens[19]),
                                    Updates.set("InnerObjects", tokens[5]),
                                    Updates.setOnInsert(
                                        Document("cadNum", cadNum)
                                            .append(
                                                "BlockID",
                                                if (cadDigits.size >= 2) cadDigits.subList(
                                                    0, 2
                                                ).joinToString(":") else cadNum
                                            )
                                            .append("cadNumInt", cadNumInt)
                                            .append("ID", cadNum)
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
                            println(cadNumInt)
                            var progress = inserted / 500000.0
                            if (progress > 0.99) progress = 0.99
                            writeProgress(progress, inserted)
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