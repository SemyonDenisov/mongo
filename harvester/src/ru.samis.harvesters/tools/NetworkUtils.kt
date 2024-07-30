package ru.samis.harvesters.tools


import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLConnection
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream


/**
 * Created by markikokik on 17.09.15.
 */
object NetworkUtils {
    var CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
    var CONTENT_TYPE_TEXT_XML = "text-xml"
    var GOOGLE_URL = "http://www.google.com/"


    @Throws(IOException::class)
    @JvmOverloads
    fun downloadUrlToString(
        address: String, message: String? = null, method: String = "GET",
        contentType: String = CONTENT_TYPE_FORM_URLENCODED, charset: Charset = Charset.forName("UTF-8")
    ): String {
        val out = ByteArrayOutputStream()
        downloadUrlToStream(address, message, method, contentType, out)
        out.close()

        return String(out.toByteArray(), charset)
    }

    @Throws(IOException::class)
    @JvmOverloads
    fun downloadUrlToStream(
        address: String, message: String? = null, method: String = "GET",
        contentType: String? = null,
        out: OutputStream? = null, bufferSize: Int = 1024
    ) {
        val connection: URLConnection
        val url: URL
        val httpConnection: HttpURLConnection

        try {
            url = URL(address)
            connection = url.openConnection()
            httpConnection = connection as HttpURLConnection
            httpConnection.requestMethod = method

            val msgBytes = message?.toByteArray()
            if (msgBytes != null) {
                httpConnection.setRequestProperty("Content-Type", contentType)
                httpConnection
                    .setRequestProperty("Content-Length", msgBytes.size.toString())
                httpConnection.doOutput = true
            } else {
                httpConnection.doOutput = false
            }

            httpConnection.setRequestProperty("Accept-Encoding", "gzip")
            httpConnection.setRequestProperty(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36"
            )
            httpConnection.setRequestProperty("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
//            httpConnection.setRequestProperty("", "")
//            httpConnection.setRequestProperty("", "")
//            httpConnection.setRequestProperty("", "")
//            httpConnection.setRequestProperty("", "")
//            httpConnection.setRequestProperty("", "")
            httpConnection.doInput = true


            httpConnection.connectTimeout = 180000
            httpConnection.readTimeout = 180000

            httpConnection.connect()

            if (msgBytes != null) {
                val os = httpConnection.outputStream
                os.write(msgBytes)

                os.flush()
                os.close()
            }

            val responseCode = httpConnection.responseCode
            if (responseCode == HttpURLConnection.HTTP_OK) {
                val length = (httpConnection.contentLength.toDouble() / bufferSize).toInt()

                var inputStream = httpConnection.inputStream

                val byteArrayOutputStream = ByteArrayOutputStream()
                inputStream.copyTo(byteArrayOutputStream, bufferSize)
                inputStream.close()
                val read = byteArrayOutputStream.toByteArray()
                inputStream = ByteArrayInputStream(read)

                if ("gzip" == httpConnection.contentEncoding) {
                    //                    Log.e("downloadUrlToStream", "gzipped input");
                    inputStream = GZIPInputStream(inputStream)
                }

                val buffer = ByteArray(bufferSize)
                var c = inputStream.read(buffer)
                var i = 0
                while (c > -1) {
                    out?.write(buffer, 0, c)
                    i++
                    c = inputStream.read(buffer)
                }

                inputStream.close()
                httpConnection.disconnect()
            } else {
                throw IOException("HTTP Error: code $responseCode")
            }
        } catch (ex: IOException) {
//            ex.printStackTrace()
            throw ex
        }

    }

    @Throws(IOException::class)
    fun getContentLength(address: String): Int {
        val connection: URLConnection
        val url: URL
        val httpConnection: HttpURLConnection

        url = URL(address)
        connection = url.openConnection()
        httpConnection = connection as HttpURLConnection
        httpConnection.requestMethod = "GET"

        httpConnection.doInput = true


        httpConnection.connectTimeout = 15000
        httpConnection.readTimeout = 14000

        httpConnection.connect()

        val responseCode = httpConnection.responseCode
        if (responseCode == HttpURLConnection.HTTP_OK) {
            val length = httpConnection.contentLength

            httpConnection.disconnect()
            return length
        } else {
            return 0
        }
    }

    class InternetDownException : IOException("google.com unavailable")
}
