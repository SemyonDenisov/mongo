package ru.samis.harvesters.tools

import java.io.File
import java.util.*

/**
 * calcs point of intersection for segments [x11, y11]-[x12, y12] and [x21, y21]-[x22, y22]
 *
 * @param x11
 * @param y11
 * @param x12
 * @param y12
 * @param x21
 * @param y21
 * @param x22
 * @param y22
 * @return
 */
fun segmentsIntersectPoint(
    x11: Double, y11: Double, x12: Double, y12: Double,
    x21: Double, y21: Double, x22: Double, y22: Double
): DoubleArray? {
    var x11 = x11
    var y11 = y11
    var x12 = x12
    var y12 = y12
    var x21 = x21
    var y21 = y21
    var x22 = x22
    var y22 = y22
    val K1 = (y12 - y11) / (x12 - x11)
    val d1 = (x12 * y11 - x11 * y12) / (x12 - x11)
    val K2 = (y22 - y21) / (x22 - x21)
    val d2 = (x22 * y21 - x21 * y22) / (x22 - x21)
    val x = (d2 - d1) / (K1 - K2)
    val y = K1 * x + d1

    var tmp: Double
    if (x12 < x11) {
        tmp = x11
        x11 = x12
        x12 = tmp
    }
    if (y12 < y11) {
        tmp = y11
        y11 = y12
        y12 = tmp
    }
    if (x22 < x21) {
        tmp = x21
        x21 = x22
        x22 = tmp
    }
    if (y22 < y21) {
        tmp = y21
        y21 = y22
        y22 = tmp
    }

    return if (x in x11..x12 && x in x21..x22 &&
        y in y11..y12 && y in y21..y22
    ) {
        doubleArrayOf(x, y)
    } else {
        null
    }
}


//                String regex = "[^0-9А-Яа-яёЁ]";
private val calendar = Calendar.getInstance().apply { timeZone = TimeZone.getTimeZone("GMT+4:00") }
val DAY_BOUND_HOUR = 3
val DAY_SECS = 24 * 60 * 60
val DAY_MILLIS = DAY_SECS * 1000L
val DAY_OFFSET_SECS = DAY_BOUND_HOUR * 60 * 60
val DAY_OFFSET_MILLIS = DAY_OFFSET_SECS * 1000L


/**
 * @param dateYYYYMMDD
 * @return interval from 3:00 day matching given date to 3:00 of next day
 */
fun getDayLimits(dateYYYYMMDD: String): IntArray {
    val year = Integer.parseInt(dateYYYYMMDD.substring(0, 4))
    val month = Integer.parseInt(dateYYYYMMDD.substring(4, 6))
    val day = Integer.parseInt(dateYYYYMMDD.substring(6, 8))
    return getDayLimits(year, month, day)
}

/**
 * @param limits
 * @return array of timestamps from limits[0] to limits[1] with intermediate values every 3:00
 */
fun splitByDays(limits: IntArray): IntArray {
    var firstIndex = 0
    val firstDayEnd = getDayEnd(limits[0])
    if (firstDayEnd > limits[0]) {
        firstIndex++
    }
    val rest = (limits[1] - firstDayEnd) % DAY_SECS
    var count = (limits[1] - firstDayEnd - rest) / DAY_SECS + 1
    if (rest > 0) {
        count++
    }
    val result = IntArray(firstIndex + count)
    result[0] = limits[0]
    result[result.size - 1] = limits[1]
    var time = firstDayEnd
    while (time < limits[1]) {
        result[firstIndex++] = time
        time += DAY_SECS
    }
    return result
}

/**
 * @param year
 * @param month
 * @param day
 * @return timestamp matching 12:00 of given day
 */
fun getMiddayTimestamp(year: Int, month: Int, day: Int): Int {
    val calendar = Calendar.getInstance()
    calendar.timeZone = TimeZone.getTimeZone("GMT+4:00")
    calendar.set(year, month, day, 12, 0)
    return (calendar.timeInMillis / 1000).toInt()
}

/**
 * @param year
 * @param month
 * @param day
 * @return timestamps from 3:00 day matching given date to 3:00 of next day
 */
fun getDayLimits(year: Int, month: Int, day: Int): IntArray {
    val result = IntArray(2)
    calendar.set(year, month - 1, day, DAY_BOUND_HOUR, 0, 0)
    result[0] = (calendar.timeInMillis / 1000).toInt()
    calendar.timeInMillis = calendar.timeInMillis + DAY_MILLIS
    result[1] = (calendar.timeInMillis / 1000).toInt()
    return result
}

/**
 * @param time
 * @return timestamp matching next 3:00
 */
fun getDayEnd(time: Int): Int {
    return getDayBeginning(time) + DAY_SECS
}

/**
 * @param time
 * @return timestamp matching next 3:00
 */
fun getDayEnd(year: Int, month: Int, day: Int): Int {
    return getDayBeginning(year, month, day) + DAY_SECS
}

/**
 * @param time
 * @return timestamp matching 3:00 of given timestamp's day
 */
fun getDayBeginning(time: Int): Int {
    return getHourTimestampOfDay(time, DAY_BOUND_HOUR)
}

/**
 * @param year
 * @param month
 * @param day
 * @return timestamp matching 3:00
 */
fun getDayBeginning(year: Int, month: Int, day: Int): Int {
    calendar.set(year, month - 1, day, DAY_BOUND_HOUR, 0, 0)
    return (calendar.timeInMillis / 1000).toInt()
}

/**
 * @param time
 * @return timestamp of 12:00 of day matching given timestamp
 */
fun getMidday(time: Int): Int {
    return getHourTimestampOfDay(time, 12)
}

/**
 * @param time
 * @return timestamp of given hour of day matching given timestamp
 */
fun getHourTimestampOfDay(time: Int, hour: Int): Int {
    calendar.timeInMillis = time * 1000L - DAY_OFFSET_MILLIS

    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    return (calendar.timeInMillis / 1000).toInt()
}


/**
 * @param a
 * @param b
 * @return a*a+b*b
 */
fun sumSq(a: Double, b: Double): Double {
    return a * a + b * b
}


/** Helper function to convert a rotation vector to a rotation matrix.
 * Given a rotation vector (presumably from a ROTATION_VECTOR sensor), returns a
 * 9  or 16 element rotation matrix in the array R.  R must have length 9 or 16.
 * If R.length == 9, the following matrix is returned:
 * <pre>
 * /  R[ 0]   R[ 1]   R[ 2]   \
 * |  R[ 3]   R[ 4]   R[ 5]   |
 * \  R[ 6]   R[ 7]   R[ 8]   /
</pre> *
 * If R.length == 16, the following matrix is returned:
 * <pre>
 * /  R[ 0]   R[ 1]   R[ 2]   0  \
 * |  R[ 4]   R[ 5]   R[ 6]   0  |
 * |  R[ 8]   R[ 9]   R[10]   0  |
 * \  0       0       0       1  /
</pre> *
 * @param rotationVector the rotation vector to convert
 * @param R an array of floats in which to store the rotation matrix
 */
fun getRotationMatrixFromVector(R: FloatArray, rotationVector: FloatArray) {

    var q0: Float
    val q1 = rotationVector[0]
    val q2 = rotationVector[1]
    val q3 = rotationVector[2]

    if (rotationVector.size >= 4) {
        q0 = rotationVector[3]
    } else {
        q0 = 1f - q1 * q1 - q2 * q2 - q3 * q3
        q0 = if (q0 > 0) Math.sqrt(q0.toDouble()).toFloat() else 0f
    }

    val sq_q1 = 2f * q1 * q1
    val sq_q2 = 2f * q2 * q2
    val sq_q3 = 2f * q3 * q3
    val q1_q2 = 2f * q1 * q2
    val q3_q0 = 2f * q3 * q0
    val q1_q3 = 2f * q1 * q3
    val q2_q0 = 2f * q2 * q0
    val q2_q3 = 2f * q2 * q3
    val q1_q0 = 2f * q1 * q0

    if (R.size == 9) {
        R[0] = 1f - sq_q2 - sq_q3
        R[1] = q1_q2 - q3_q0
        R[2] = q1_q3 + q2_q0

        R[3] = q1_q2 + q3_q0
        R[4] = 1f - sq_q1 - sq_q3
        R[5] = q2_q3 - q1_q0

        R[6] = q1_q3 - q2_q0
        R[7] = q2_q3 + q1_q0
        R[8] = 1f - sq_q1 - sq_q2
    } else if (R.size == 16) {
        R[0] = 1f - sq_q2 - sq_q3
        R[1] = q1_q2 - q3_q0
        R[2] = q1_q3 + q2_q0
        R[3] = 0.0f

        R[4] = q1_q2 + q3_q0
        R[5] = 1f - sq_q1 - sq_q3
        R[6] = q2_q3 - q1_q0
        R[7] = 0.0f

        R[8] = q1_q3 - q2_q0
        R[9] = q2_q3 + q1_q0
        R[10] = 1f - sq_q1 - sq_q2
        R[11] = 0.0f

        R[14] = 0.0f
        R[13] = R[14]
        R[12] = R[13]
        R[15] = 1.0f
    }
}




fun angleDiff(angle1: Float, angle2: Float): Float {
    var diff = Math.abs(angle1 - angle2)
    if (diff > 180)
        diff = Math.abs(diff - 360)
    return diff
}

fun listFilesRecursive(dir: File, ext: String): ArrayList<File> {
    val result = arrayListOf<File>()
    for (subDir in dir.listFiles { file -> file.isDirectory }) {
        result.addAll(listFilesRecursive(subDir, ext))
    }
    val files = dir.listFiles { file -> file.extension == ext }
    result.addAll(files)
    return result

}

fun <T> measureTime(label: String = "", block: () -> T): T {
    var time = System.nanoTime()
    val result = block()
    time = System.nanoTime() - time
    time /= 1000000
    print(label)
    if (label.isNotBlank())
        print(" ")
    println("time: $time")
    return result
}