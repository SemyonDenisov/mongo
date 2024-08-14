package ru.samis.addressparser
/*
import org.json.JSONObject
import java.io.File
import java.sql.DriverManager
import java.sql.Statement


class MssqlAddressParser : AddressParser(null) {

    fun processAddressesInMssql() {
        val settings = JSONObject(File("settings.json").readText())
        val mssqlParams = settings.getJSONObject("mssql")
        val connectionUrl = "jdbc:sqlserver://${mssqlParams.getString("url")};" +
                "database=${mssqlParams.getString("db")};" +
                "user=${mssqlParams.getString("user")};" +
                "password=${mssqlParams.getString("pass")};" +
                "encrypt=true;" +
                "trustServerCertificate=true;" +
                "loginTimeout=30;"


        DriverManager.getConnection(connectionUrl).use { connection ->
            val statement = connection.createStatement()

            val withTable = { table: String ->
                statement.execute(
                    "alter table Nalogi2021.dbo.$table add K_Region nvarchar(255), K_NasPunkt nvarchar(255), K_Street nvarchar(255), K_HouseNumber nvarchar(255), K_Flat nvarchar(255)"
                )
                val resultSet = statement.executeQuery("select count(*) from $table")
                resultSet.next()
                val totalCount = resultSet.getInt(1)
                recognizeAddressesInTable(statement, table, "select CN,adr from $table", "CN", totalCount)
            }

            "oks_bez_prav_dop".let { table ->
                statement.execute(
                    "alter table Nalogi2021.dbo.$table add K_Region nvarchar(255), K_NasPunkt nvarchar(255), K_Street nvarchar(255), K_HouseNumber nvarchar(255), K_Flat nvarchar(255)"
                )
                val resultSet = statement.executeQuery("select count(*) from $table where srcRayon is not null")
                resultSet.next()
                val totalCount = resultSet.getInt(1)
                recognizeAddressesInTable(
                    statement,
                    table,
                    "select CN,adr from $table where srcRayon is not null",
                    "CN",
                    totalCount
                )
            }

            withTable("zu_rvp")
            withTable("zu_bez_prav_dop2")
            withTable("rooms_bez_prav_dop")
        }
    }

    fun recognizeAddressesInTable(
        statement: Statement,
        table: String,
        selectSql: String,
        cadNumField: String = "KadN_Obj",
        totalCount: Int = 0
    ) {
//    var resultSet = statement.executeQuery("select count(KadN_Obj) from $table where K_Region is null")
//    val selectSql = "select top 10 KadN_Obj,Adres_NF from $table"
//    val selectSql = "select KadN_Obj,Adres_NF from $table where K_Region is null"
//    val selectSql = "select KadN_Obj,Adres_NF from $table where K_HouseNumber is null"
        val resultSet = statement.executeQuery(selectSql)

//        val metaData = resultSet.metaData
//        for (i in 1..metaData.columnCount) {
//            print("${resultSet.metaData.getColumnLabel(i)} ")
//        }

        var data = arrayListOf<String>()
        val cadNums = arrayListOf<String>()
        val addresses = arrayListOf<String>()
        var count = 0
        var cadIndex = 0

        val inserter = { count: Int ->
//            val isError = recognizeAddresses(
//                data,
//                count,
//                { region, blockID, municipality, street, houseNum, flatNumber, notDecomposed, original ->
//                    println(cadIndex)
//                    println(cadNums[cadIndex])
//                    println(addresses[cadIndex])
//                    println("$blockID $municipality $street $houseNum $flatNumber ($notDecomposed)")
//                    val updateSql = "update $table set " +
//                            "K_Region='$region', " +
//                            "K_NasPunkt='$municipality', " +
//                            "K_Street='$street', " +
//                            "K_HouseNumber='$houseNum', " +
//                            "K_Flat='$flatNumber' " +
//                            "where $cadNumField='${cadNums[cadIndex]}'"
////            println(updateSql)
//
//                    try {
//                        statement.executeUpdate(updateSql)
//                    } catch (e: Exception) {
//                        e.printStackTrace()
//                    }
//                    cadIndex++
//                }) { e, line ->
//                cadIndex++
//                e.printStackTrace()
//                println(line)
//            }

            data.clear()
//            isError
        }

        while (resultSet.next()) {
            val address = resultSet.getString(2) ?: continue
            if (address.isBlank()) continue
            addresses += address
            cadNums += resultSet.getString(1)
        }

        var isError = false
        for (i in cadIndex until addresses.size) {
//            val address = prepareAddress(addresses[i])
//
//            data.add(address)

            count++
            if (count % PORTION_SIZE == 0) {
//                isError = inserter(PORTION_SIZE)
                println("$count / $totalCount processed (${(100.0 * count / totalCount).toInt()}%)")
                if (isError)
                    break
            }
        }

        if (count % PORTION_SIZE != 0 && !isError) {
            inserter(count % PORTION_SIZE)
            println("$count / $totalCount processed (${(100.0 * count / totalCount).toInt()}%)")
        }
    }
}
*/