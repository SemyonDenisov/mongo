package ru.samis.harvesters

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import kotlin.system.measureTimeMillis


class YandexHarvester : FragmentationHarvester() {

    private val sourceDb = client.getDatabase(options.getString("rawDatabase"))
    private val sourceCollection = sourceDb.getCollection(options.getString("rawDataset"))

    override fun mainHarvest(): Int {

        var inserted = 0

        housesCollection.apply {
            drop()
            createIndex(Indexes.ascending("ID"))
            createIndex(Indexes.ascending("OriginID"))
            createIndex(Indexes.ascending("RegionID"))
            createIndex(Indexes.ascending("BlockID"))
            createIndex(Indexes.ascending("Region"))
            createIndex(Indexes.geo2dsphere("Geometry"))
        }

        // находим границу региона
        var regionDocument = Document()
        try {
            regionDocument = regionsCollection.find(
                Filters.and(
                    Filters.eq("_id", ObjectId(regionBorderId))
                )
            ).first() ?: Document()
        } catch (e: Exception) {
            println("error: ${e.message}")
            e.message?.let { writeError(it) }
        }

        // фильтруем исходную коллекцию по региону
        if (!regionDocument.isNullOrEmpty()) {
            if (regionDocument["geometry"] != null) {
                val objectsInRegion = sourceCollection.find(
                    Filters.geoIntersects("geometry", regionDocument["geometry"] as Bson)
                ).cursor()

                val elapsedTime = measureTimeMillis {

                    var idCounter = 0

                    for (obj in objectsInRegion) {
                        var lat = 0.0
                        var lon = 0.0
                        var geomPointsCounter = 0
                        var geom: Any? = null

                        val coordinates = (obj["geometry"] as Document)["coordinates"] as List<*>
                        coordinates.forEach {
                            it as List<*>
                            geom = it
                            it.forEach { elem ->
                                elem as List<Double>
                                lat += elem[1]
                                lon += elem[0]
                                geomPointsCounter++
                            }
                        }

                        lat /= geomPointsCounter
                        lon /= geomPointsCounter

                        val x = lon2merc(lon)
                        val y = lat2merc(lat)

                        val doc = Document()
                            .append("ID", regionCode + "_" + idCounter++)
                            .append("y", y)
                            .append("x", x)
                            .append("radius", avgRaduis(geom as List<*>, x, y))
                            .append("BlockID", findArea(lat, lon))
                            .append("Region", region)
                            .append("RegionID", regionID)
                            .append("RegionCode", regionCode)
                            .append("Geometry", obj["geometry"])
                            .apply {
                                (obj["id"] as? String)?.let { id -> if (id.isNotEmpty()) append("OriginID", id) }
                                (obj["HouseNumber"] as? String)?.let { houseNumber ->
                                    if (houseNumber.isNotEmpty()) append("HouseNumber", houseNumber)
                                }
                                (obj["height"] as? Long)?.let { height -> append("height", height) }
                                obj["landuse"]?.let { landuse ->
                                    val filteredLanduse = (landuse as? List<String>)?.filter { it.isNotEmpty() }
                                    if (!filteredLanduse.isNullOrEmpty()) append("landuse", filteredLanduse)
                                }
                                obj["orgs"]?.let { orgs ->
                                    val filteredOrgs = (orgs as? List<String>)?.filter { it.isNotEmpty() }
                                    if (!filteredOrgs.isNullOrEmpty()) append("orgs", filteredOrgs)
                                }
                                obj["orgs_rubrics"]?.let { orgsRubrics ->
                                    val filteredOrgsRubrics = (orgsRubrics as? List<String>)?.filter { it.isNotEmpty() }
                                    if (!filteredOrgsRubrics.isNullOrEmpty()) append(
                                        "orgs_rubrics",
                                        filteredOrgsRubrics
                                    )
                                }
                            }
                        housesCollection.insertOne(doc)
                        inserted++
                    }
                }.toDouble()

                println("Elapsed time: ${elapsedTime / 1000} seconds")
            }
        } else {
            println("Region border not found!")
        }
        println("inserted: $inserted")
        return inserted
    }
}

