package ru.samis.harvesters

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.util.*
import java.util.zip.ZipFile
import kotlin.collections.HashMap

class ReformaGKHparser(private val fileName: String) : AutoCloseable {
    private var zf: ZipFile? = null
    private var stream: InputStream? = null
    private var reader: BufferedReader? = null
    private val captions = HashMap<String, Int>()

    fun getColumnIndex(caption: String): Int {
        return captions[caption.uppercase(Locale.getDefault())] ?: -1
    }

    fun init() {
        zf = ZipFile(fileName).apply {
            reader = InputStreamReader(getInputStream(entries().nextElement())).buffered().apply {
                val tokens = readLine().split(";")
                for ((i, caption) in tokens.withIndex()) {
                    captions[caption.trim { !it.isLetterOrDigit() }.uppercase(Locale.getDefault())] = i
                }
            }
        }
    }

    fun next(): String? {
        return reader?.readLine()
    }

    override fun close() {
        zf?.close()
        stream?.close()
    }
}