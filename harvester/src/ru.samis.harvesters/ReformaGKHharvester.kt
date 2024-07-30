package ru.samis.harvesters

import com.mongodb.client.model.Indexes
import org.bson.Document
import java.util.*

class ReformaGKHharvester : Harvester() {


    override fun mainHarvest(): Int {
        try {
            insertMetadata(datasetStructure)

            with(housesCollection) {
                drop()

                createIndex(Indexes.ascending("ID"))
                createIndex(Indexes.ascending("RegionID"))
                createIndex(Indexes.ascending("MunicipalitetID"))
                createIndex(Indexes.ascending("StreetID"))
                createIndex(Indexes.ascending("BlockID"))
                createIndex(
                    Indexes.compoundIndex(
                        Indexes.text("Region"),
                        Indexes.text("Municipalitet"),
                        Indexes.text("Street"),
                        Indexes.text("HouseNumber")
                    )
                )
            }

            val parser = ReformaGKHparser(
                settings.getJSONObject("options").getString("reformagkhCatalog") +
                        settings.getJSONObject("params").getString("regionFile")
            )
            parser.init()

            val shortnameAreaIndex = parser.getColumnIndex("shortname_area")
            val formalnameAreaIndex = parser.getColumnIndex("formalname_area")
            val shortnameCityIndex = parser.getColumnIndex("shortname_city")
            val formalnameCityIndex = parser.getColumnIndex("formalname_city")
            val shortnameStreetIndex = parser.getColumnIndex("shortname_street")
            val formalnameStreetIndex = parser.getColumnIndex("formalname_street")
            val houseNumberIndex = parser.getColumnIndex("house_number")
            val buildingIndex = parser.getColumnIndex("building")
            val blockIndex = parser.getColumnIndex("block")
            val letterIndex = parser.getColumnIndex("letter")

            val matchingIndexes = mutableMapOf<Int, Array<String>>()
            for (entry in fieldsMatching) {
                matchingIndexes[parser.getColumnIndex(entry[0])] = entry
            }

            var line = parser.next()
            var count = 0
            val length = UNFILLED.length
            while (line != null) {
                try {
                    var tokens = line.split(";")
                    if (tokens.size < letterIndex) {
                        val list = tokens.toMutableList()
                        while (list.size < letterIndex && line != null) {
                            line = parser.next()
                            line?.split(";")?.let {
                                list += it
                            }
                        }
                        tokens = list
                    }
                    if (tokens.size < letterIndex) break

                    val document = Document()
                    for ((i, descr) in matchingIndexes) {
                        val indexUnfilled = tokens[i].toLowerCase().indexOf(UNFILLED)
                        var valueStr = tokens[i]
                        if (indexUnfilled >= 0)
                            valueStr = tokens[i].replaceRange(indexUnfilled, indexUnfilled + length, "")
                        valueStr = valueStr.replace("\"", "")
                            .replace("&quot", "").replace(",", ".")
                            .trim()
                        val value = when (descr[2]) {
                            "Int" -> valueStr.toIntOrNull() ?: 0
                            "Double" -> valueStr.toDoubleOrNull() ?: 0.0
                            else -> valueStr
                        }
                        document.append(descr[1], value)
                    }

                    document.append(
                        "Municipalitet",
                        (tokens[shortnameAreaIndex] + " " + tokens[formalnameAreaIndex] + " "
                                + tokens[shortnameCityIndex] + " " + tokens[formalnameCityIndex])
                            .replace("\"", "")
                            .replace("&quot", "")
                            .trim()
                    )

                    document.append(
                        "BlockID",
                        (if (tokens[formalnameAreaIndex].isNotEmpty()) tokens[formalnameAreaIndex] else tokens[formalnameCityIndex])
                            .replace("\"", "")
                            .replace("&quot", "")
                    )

                    document.append(
                        "Street",
                        (tokens[shortnameStreetIndex] + " " + tokens[formalnameStreetIndex])
                            .replace("\"", "")
                            .replace("&quot", "")
                            .trim()
                    )

                    document.append(
                        "HouseNumber",
                        (tokens[houseNumberIndex] + " " + tokens[buildingIndex] + tokens[blockIndex] + tokens[letterIndex]).trim()
                    )

                    document.append("PostalCode", null)
                    document.append("RegionCode", regionCode)
                    document.append("x", null)
                    document.append("y", null)
                    document.append("Geometry", Document("type", "WTF").append("coordinates", null))

                    housesCollection.insertOne(document)
                    count++
                    if (count % 100 == 0) writeProgress(0.1, count)
                } catch (e: Exception) {
                    writeError(e.message ?: "")
                }

                line = parser.next()
            }

            parser.close()

            return count
        } catch (e: Exception) {
            writeError(e.message ?: "")
        }
        return 0
    }

    companion object {
        private val fieldsMatching = arrayOf(
            arrayOf("id", "ID", "String"),
            arrayOf("formalname_region", "Region", "String"),
            arrayOf("region_id", "RegionID", "String"),
            arrayOf("city_id", "MunicipalitetID", "String"),
            arrayOf("street_id", "StreetID", "String"),
            arrayOf("address", "AddressDesc", "String"),
            arrayOf("built_year", "builtYear", "Int"),
            arrayOf("exploitation_start_year", "exploitationStartYear", "Int"),
            arrayOf("project_type", "projectType", ""),
            arrayOf("house_type", "houseType", ""),
            arrayOf("floor_count_max", "floorsCount", "Int"),
            arrayOf("entrance_count", "entranceCount", "Int"),
            arrayOf("elevators_count", "elevatorsCount", "Int"),
            arrayOf("quarters_count", "flatsCount", "Int"),
            arrayOf("living_quarters_count", "livingFlatsCount", "Int"),
            arrayOf("unliving_quarters_count", "unlivingFlatsCount", "Int"),
            arrayOf("area_total", "areaTotal", "Double"),
            arrayOf("area_non_residential", "areaNonResidential", "Double"),
            arrayOf("area_residential", "areaResidential", "Double"),
            arrayOf("area_common_property", "areaCommonProperty", "Double"),
            arrayOf("area_land", "areaLand", "Double"),
            arrayOf("parking_square", "parkingSquare", "Double"),
            arrayOf("playground", "playground", "Int"),
            arrayOf("sportsground", "sportsground", "Int"),
            arrayOf("other_beautification", "otherBeautification", ""),
            arrayOf("foundation_type", "foundationType", ""),
            arrayOf("floor_type", "floorType", ""),
            arrayOf("wall_material", "wallMaterial", ""),
            arrayOf("basement_area", "basementArea", ""),
            arrayOf("chute_type", "chuteType", ""),
            arrayOf("chute_count", "chuteCount", "Int"),
            arrayOf("electrical_type", "electricalType", ""),
            arrayOf("electrical_entries_count", "electricalEntriesCount", "Int"),
            arrayOf("heating_type", "heatingType", ""),
            arrayOf("hot_water_type", "hotWaterType", ""),
            arrayOf("cold_water_type", "coldWaterType", ""),
            arrayOf("sewerage_type", "sewerageType", ""),
            arrayOf("sewerage_cesspools_volume", "sewerageCesspoolsVolume", ""),
            arrayOf("gas_type", "gasType", ""),
            arrayOf("ventilation_type", "ventilationType", ""),
            arrayOf("firefighting_type", "firefightingType", ""),
            arrayOf("drainage_type", "drainageType", "")
        )

        val UNFILLED = "не заполнено"

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