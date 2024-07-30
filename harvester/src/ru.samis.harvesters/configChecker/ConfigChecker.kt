package ru.samis.harvesters.configChecker

import org.json.JSONObject
import java.io.File
import kotlin.system.exitProcess

class ConfigChecker {
    private val requiredBaseFields = listOf("ConnectionString", "database", "dataset", "RegionCode")
    private val requiredAddressFields = listOf("datasetFile", "cudaOutFile", "cudaParserDir", "runYandex",
                                                            "elasticParserDir", "elasticOutFile", "elasticHost", "elasticPort",
                                                            "threads", "elasticScheme", "deepparseDir", "deepparseOutFile",
                                                            "libpostalDir", "libpostalOutFile",  "geonormCatalog",
                                                            "natashaOutFile", "unitedOutFile", "parsersIndexes", "fieldsBest",
                                                            "fieldsElse", "specialWords", "algorithmsForCommonValue",
                                                            "ConnectionString", "commonDatabase")


    val settings = JSONObject(File("settings.json").readText())
    val options: JSONObject = settings.optJSONObject("options", JSONObject())
    val params: JSONObject = settings.optJSONObject("params", JSONObject())

    fun checkBaseConfig() {
        var missingFields = false
        if (params != null && !params.isEmpty) {
            for (field in requiredBaseFields) {
                val value = params.optString(field, null)
                if (value == null || value.isEmpty()) {
                    println("$field is not configured!")
                    missingFields = true
                }
            }
        } else {
            println("params are not set!")
        }
        if (missingFields) {
            exitProcess(1)
        }
    }

    fun checkAddressConfig() {
        if ((params != null && !params.isEmpty) || (options != null && !options.isEmpty)) {
            println("params or options are not set!")
            exitProcess(1)
        }
        val parseAddress = params.optBoolean("parseAddress", false)
        var missingFields = false


        if (parseAddress) {
            for (field in requiredAddressFields) {
                val value = options.optString(field, null)
                if (value == null || value.isEmpty()) {
                    println("$field is not configured!")
                    missingFields = true
                }
            }
            if (missingFields) {
                exitProcess(1)
            }
        }
    }

    fun checkCustomConfig(requiredCustomFields: List<String>) {
        var missingFields = false
        for (field in requiredCustomFields) {
            val value = params.optString(field, null)
            if (value == null || value.isEmpty()) {
                println("$field is not configured!")
                missingFields = true
            }
        }

        if (missingFields) {
            exitProcess(1)
        }

    }


}