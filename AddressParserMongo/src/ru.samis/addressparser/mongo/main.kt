package ru.samis.addressparser.mongo


import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Filters
import org.bson.UuidRepresentation
import org.json.JSONObject
import ru.samis.addressparser.AddressParser
import java.io.File
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    AddressParser().parseAddresses { count, total ->
        println("processed $count of $total")
    }
    exitProcess(0)

}