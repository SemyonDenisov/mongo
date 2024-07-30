package ru.samis.harvesters

import com.mongodb.client.model.Indexes
import org.bson.Document
import java.io.BufferedReader
import java.io.InputStreamReader
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import kotlin.system.exitProcess

@Deprecated("Use DGISCSVHarvester")
class DGISHarvester : FragmentationHarvester() {

    override fun mainHarvest(): Int {
        insertMetadata(datasetStructure)
        settings.getJSONObject("params").optString("regionFile")?.let {
            if (it.isNotEmpty())
                restoreDB(
                    settings.getJSONObject("options").getString("2gisCatalog") + it
                )
        }

        connect().use { c ->
            c.createStatement().use { s ->
                s.executeQuery(QUERY).use { resultSet ->
                    housesCollection.drop()
                    val idIndex = resultSet.findColumn("ID")
                    val regionIndex = resultSet.findColumn("Region")
                    val regionIdIndex = resultSet.findColumn("RegionID")
                    val municipalitetIndex = resultSet.findColumn("Municipalitet")
                    val municipalitetIdIndex = resultSet.findColumn("MunicipalitetID")
//                    val blockIdIndex = resultSet.findColumn("BlockID")
                    val streetIndex = resultSet.findColumn("Street")
                    val streetIdIndex = resultSet.findColumn("StreetID")
                    val houseNumberIndex = resultSet.findColumn("HouseNumber")
                    val postalCodeIndex = resultSet.findColumn("PostalCode")
                    val xIndex = resultSet.findColumn("x")
                    val yIndex = resultSet.findColumn("y")
                    val latIndex = resultSet.findColumn("lat")
                    val lonIndex = resultSet.findColumn("lon")
                    while (resultSet.next()) {
                        val coords = listOf(
                            resultSet.getDouble(lonIndex), resultSet.getDouble(latIndex)
                        )

                        if (!isInRegion(coords[1], coords[0])) continue

                        val districtName = findArea(coords[1], coords[0])

                        housesCollection.insertOne(
                            Document()
                                .append("ID", resultSet.getString(idIndex))
                                .append("Region", resultSet.getString(regionIndex))
                                .append("RegionID", resultSet.getInt(regionIdIndex))
                                .append("Municipalitet", resultSet.getString(municipalitetIndex))
                                .append("MunicipalitetID", resultSet.getInt(municipalitetIdIndex))
//                                .append("BlockID", resultSet.getInt(blockIdIndex))
                                .append("BlockID", districtName)
                                .append("Solid", 1)
                                .append("Street", resultSet.getString(streetIndex))
                                .append("StreetID", resultSet.getInt(streetIdIndex))
                                .append("HouseNumber", resultSet.getString(houseNumberIndex))
                                .append("PostalCode", resultSet.getString(postalCodeIndex))
                                .append("x", resultSet.getDouble(xIndex))
                                .append("y", resultSet.getDouble(yIndex))
                                .append(
                                    "Geometry",
                                    Document()
                                        .append("type", "Point")
                                        .append(
                                            "coordinates",
                                            coords
                                        )
                                )
                        )
                    }
                }
            }

        }



        housesCollection.createIndex(Indexes.ascending("ID"))
        housesCollection.createIndex(Indexes.ascending("RegionID"))
        housesCollection.createIndex(Indexes.ascending("MunicipalitetID"))
        housesCollection.createIndex(Indexes.ascending("StreetID"))
        housesCollection.createIndex(Indexes.ascending("BlockID"))
        housesCollection.createIndex(
            Indexes.compoundIndex(
                Indexes.text("Region"),
                Indexes.text("Municipalitet"),
                Indexes.text("Street"),
                Indexes.text("HouseNumber")
            )
        )

        return housesCollection.countDocuments().toInt()
    }

    @Throws(ClassNotFoundException::class, SQLException::class)
    fun connect(): Connection {
        Class.forName("com.mysql.jdbc.Driver")
        settings.getJSONObject("options").let {
            val user = it.optString("MysqlUser")
            val pass = it.optString("MysqlPass")
            val host = it.optString("MysqlHost")
            val db = it.optString("MysqlDatabase")
            val c = DriverManager.getConnection(
                "jdbc:mysql://$host/$db?user=$user&password=$pass"
            )
            c.autoCommit = false
            return c
        }
    }

    fun restoreDB(path: String) {
        settings.getJSONObject("options").let {
            val user = it.optString("MysqlUser")
            val pass = it.optString("MysqlPass")
            val host = it.optString("MysqlHost")

            val restoreCmd = if (System.getProperty("os.name").toLowerCase().contains("linux"))
                arrayOf("sh", "-c", "mysql -u $user -p$pass -h$host < $path")
            else
                arrayOf("cmd", "/c", "mysql -u $user -p$pass -h$host < $path")
//    val restoreCmd = "sh -c ls /home"

            val runtimeProcess: Process
            try {

                runtimeProcess = Runtime.getRuntime().exec(restoreCmd)

                val br = BufferedReader(
                    InputStreamReader(runtimeProcess.errorStream)
                )
                var s = br.readLine()
                while (s != null) {
//                    System.out.println("line: $s")
                    s = br.readLine()
                }


                val processComplete = runtimeProcess.waitFor()

                connect().use { c ->
                    c.createStatement().use { s ->
                        s.execute("ALTER TABLE `addresses` ADD INDEX(`lat`);")
                        s.execute("ALTER TABLE `addresses` ADD INDEX(`lon`);")
                        s.execute("ALTER TABLE `addresses` ADD INDEX( `lat`, `lon`);")
                    }
                    c.commit()
                }

                if (processComplete == 0) {
                    println("Restored successfully!")
                    writeProgress(0.1, 0)
                } else {
                    println("Could not restore the backup!")
                    writeError("Could not restore the backup!")
                    exitProcess(1)
                }
            } catch (ex: Exception) {
                ex.printStackTrace()
            }
        }
    }

    companion object {
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

        val QUERY = "SELECT\n" +
                "                (case when isNull(d.id1)>0 then h.id_address else d.id1 end) as 'ID',\t\t\t\t\n" +
                "                r.region as 'Region',\n" +
                "                r.id_region as 'RegionID',\n" +
                "                t.town as 'Municipalitet',\n" +
                "                t.id_town as 'MunicipalitetID',\n" +
                "                s.id_town as 'BlockID',\n" +
                "                1 as 'Solid',\n" +
                "                s.street as 'Street',\n" +
                "                s.id_street as 'StreetID',\n" +
                "                number as 'HouseNumber',\n" +
                "                post_index as 'PostalCode',\n" +
                "                (CASE WHEN lon > 180 THEN NULL ELSE lon * 0.017453292519943295 * 6378137.0 END) as 'x', \n" +
                "                (CASE WHEN lat > 90 THEN NULL ELSE 3189068.5 * LOG((1.0 + SIN(lat * 0.017453292519943295)) / (1.0 - SIN(lat * 0.017453292519943295))) END)  as 'y',\n" +
                "                lat,lon\n" +
                "                FROM addresses h\n" +
                "                join streets s on (s.id_street=h.id_street)\n" +
                "                join towns t on (t.id_town=s.id_town)\n" +
                "                join regions r on (r.id_region=t.id_region)\n" +
                "                left join( select\n" +
                "                    h.id_address as id1,\n" +
                "                    h2.id_address as id2\n" +
                "                    from addresses h\n" +
                "                    join addresses h2 on ((h.lat=h2.lat)and(h.lon=h2.lon))  \n" +
                "                    where\n" +
                "                    (h.id_address < h2.id_address)\n" +
                "                ) d on (h.id_address = d.id2)"
    }
}

//SELECT
//(case when isNull(d.id1)>0 then h.id_address else d.id1 end) as 'ID',
//r.region as 'Region',
//r.id_region as 'RegionID',
//t.town as 'Municipalitet',
//t.id_town as 'MunicipalitetID',
//s.id_town as 'BlockID',
//1 as 'Solid',
//s.street as 'Street',
//s.id_street as 'StreetID',
//number as 'HouseNumber',
//post_index as 'PostalCode',
//(CASE WHEN lon > 180 THEN NULL ELSE lon * 0.017453292519943295 * 6378137.0 END) as 'x',
//(CASE WHEN lat > 90 THEN NULL ELSE 3189068.5 * LOG((1.0 + SIN(lat * 0.017453292519943295)) / (1.0 - SIN(lat * 0.017453292519943295))) END)  as 'y',
//lat,lon
//FROM addresses h
//join streets s on (s.id_street=h.id_street)
//join towns t on (t.id_town=s.id_town)
//join regions r on (r.id_region=t.id_region)
//left join( select
//h.id_address as id1,
//h2.id_address as id2
//from addresses h
//join addresses h2 on ((h.lat=h2.lat)and(h.lon=h2.lon))
//where
//(h.id_address < h2.id_address)
//) d on (h.id_address = d.id2)