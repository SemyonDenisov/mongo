package ru.samis.addressparser

import kotlin.system.exitProcess


fun main(args: Array<String>) {
    UnitedParser().parse("service_address.txt", "united_out.jsonl")

    println("end main")
    exitProcess(0)
}