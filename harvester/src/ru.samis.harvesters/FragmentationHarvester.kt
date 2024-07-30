package ru.samis.harvesters

import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Position
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document
import org.bson.types.ObjectId

abstract class FragmentationHarvester : Harvester() {
    protected val region = params.getJSONObject("blockFragmentation").getString("region")!!
    protected val regionID = DigestUtils.sha1Hex(region)
    protected val regionBorderId = params.getJSONObject("regionBorder").getString("id")
    protected val regionBorderDb = params.getJSONObject("regionBorder").getString("database")
    protected val regionBorderDataset = params.getJSONObject("regionBorder").getString("dataset")

    private val blockFragmentationDb = client
        .getDatabase(params.getJSONObject("blockFragmentation").getString("database"))
    protected val regionMunicipalicies = blockFragmentationDb
        .getCollection(params.getJSONObject("blockFragmentation").getString("dataset"))
        .find(eq("region", region))

    protected val regionSettlements = blockFragmentationDb
        .getCollection(params.getJSONObject("blockFragmentation").getString("settlementsDataset"))
        .find(eq("region", region))

    protected val regionsCollection = client
        .getDatabase(regionBorderDb)
        .getCollection(regionBorderDataset)

    protected fun findArea(lat: Double, lon: Double): String {
        regionMunicipalicies.filter(
            Filters.geoIntersects("geometry", Point(Position(lon, lat)))
        ).first()?.let {
            return it.getString("name")
        }
        return ""
    }

    protected fun isInRegion(lat: Double, lon: Double): Boolean {
        val foundRegion = regionsCollection.find(
            Filters.and(
                eq("_id", ObjectId(regionBorderId)),
                Filters.geoIntersects("geometry", Point(Position(lon, lat)))
            )
        ).first() ?: return false
        return foundRegion["name"] == region

    }

    protected fun findRegion(lat: Double, lon: Double, collection: MongoCollection<Document>? = null): String {
        val collection = collection ?: MongoClients
            .create(params.getString("ConnectionString"))
            .getDatabase(regionBorderDb)
            .getCollection(regionBorderDataset)
        val foundRegion = collection
            .find(
                Filters.geoIntersects("geometry", Point(Position(lon, lat)))
            ).first() ?: return ""
        return foundRegion.getString("name") ?: ""
    }
}