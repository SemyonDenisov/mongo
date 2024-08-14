package ru.samis.addressparser.mongo


import ru.samis.addressparser.AddressParser
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    AddressParser().parseAddresses { count, total ->
        println("processed $count of $total")
    }
    exitProcess(0)

}