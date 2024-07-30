package ru.samis.harvesters.tools

import ru.samis.harvesters.tools.Geometry.getRadians
import ru.samis.harvesters.tools.Geometry.normalIntersectPoint
import kotlin.math.min

object GeoTools {
    private val LATITUDE_TO_M = 111111.11111
    private val LONGITUDE_TO_M = 66684.132308
    private val LONGITUDE_SECOND_TO_M = 30.922604938271604938271604938272

    fun pointInAreaLatLng(lat: Double, lng: Double, coords: DoubleArray): Boolean {
        var x11: Double
        var x12: Double
        var y11: Double
        var y12: Double
        val count: Int
        var nextIndex: Int
        count = coords.size / 2
        val K1latlng = DoubleArray(count)
        val d1latlng = DoubleArray(count)
        for (j in 0 until count) {
            nextIndex = (j + 1) % count
            x11 = coords[j * 2]
            x12 = coords[nextIndex * 2]
            y11 = coords[j * 2 + 1]
            y12 = coords[nextIndex * 2 + 1]
            K1latlng[j] = (y12 - y11) / (x12 - x11)
            d1latlng[j] = (x12 * y11 - x11 * y12) / (x12 - x11)
        }

        return pointInAreaLatLng(lat, lng, coords, d1latlng, K1latlng)
    }

    /**
     * determines if point lies on terminal station
     *
     * @param point
     * @return
     */
    fun pointInAreaLatLng(
        lat: Double, lng: Double, coords: DoubleArray,
        d1: DoubleArray, K1: DoubleArray
    ): Boolean {
        var tmp: Double
        var x11: Double
        var x12: Double
        var y11: Double
        var y12: Double
        var x: Double
        var intersect = false
        val count: Int

        var nextIndex: Int
        count = coords.size / 2
        for (j in 0 until count) {
            nextIndex = (j + 1) % count
            y11 = coords[j * 2 + 1]
            y12 = coords[nextIndex * 2 + 1]

            if (y12 < y11) {
                tmp = y11
                y11 = y12
                y12 = tmp
            }

            if (lng < Math.min(y11, y12) || lng > Math.max(y11, y12)) {
                continue
            }

            x11 = coords[j * 2]
            x12 = coords[nextIndex * 2]
            if (x12 < x11) {
                tmp = x11
                x11 = x12
                x12 = tmp
            }

            x = (lng - d1[j]) / K1[j]

            if (x >= Math.min(x11, x12) && x <= Math.max(x11, x12) && x >= lat) {
                if (getDistSqBetweenLatLng(x, lng, x12, y12) >= 2) {
                    intersect = !intersect
                }
            }
        }

        return intersect
    }

    fun metersInLatitudeDegree(): Double {
        return LATITUDE_TO_M
    }

    fun metersInLongitudeDegree(latitude: Double): Double {
        // return LATITUDE_SECOND_TO_M * Math.cos(getRadians(latitude)) * 3600;
        return kilometersInLongitudeDegree(latitude) * 1000
    }

    fun kilometersInLongitudeDegree(latitude: Double): Double {
        return LONGITUDE_SECOND_TO_M * Math.cos(getRadians(latitude)) * 3.6
    }


    fun getDistBetweenLatLng(point: DoubleArray, fromIndex: Int, x2: Double, y2: Double): Double {
        return Math.sqrt(getDistSqBetweenLatLng(point, fromIndex, x2, y2))
    }


    fun getDistSqBetweenLatLng(point: DoubleArray, fromIndex: Int, x2: Double, y2: Double): Double {
        return getDistSqBetweenLatLng(point[fromIndex], point[fromIndex + 1], x2, y2)
    }

    fun getDistBetweenLatLng(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double {
        return Math.sqrt(getDistSqBetweenLatLng(lat1, lng1, lat2, lng2))
    }

    fun getDistSqBetweenLatLng(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double {
        return sumSq(
            (lat2 - lat1) * metersInLatitudeDegree(),
            (lng2 - lng1) * metersInLongitudeDegree(lat1)
        )
    }


    fun getDistToLineLatLng(line: DoubleArray, fromIndex: Int, lat: Double, lng: Double): Double {
        return Math.sqrt(getDistSqToLineLatLng(line, fromIndex, lat, lng))
    }


    fun getDistToLineLatLng1(line: List<Pair<Double, Double>>, fromIndex: Int, lat: Double, lng: Double): Double {
        return Math.sqrt(getDistSqToLineLatLng1(line, fromIndex, lat, lng))
    }

    fun getDistSqToLineLatLng(line: DoubleArray, fromIndex: Int, lat: Double, lng: Double): Double {
        val metersInLongitudeDegree = metersInLongitudeDegree(line[fromIndex])
        val A = 1 / ((line[fromIndex + 2] - line[fromIndex]) * metersInLatitudeDegree())
        val B = 1 / ((line[fromIndex + 1] - line[fromIndex + 3]) * metersInLongitudeDegree)
        val C = A * ((lat - line[fromIndex]) * metersInLatitudeDegree()) +
                B * ((lng - line[fromIndex + 1]) * metersInLongitudeDegree)

        return C * C / sumSq(A, B)
    }


    fun getDistSqToLineLatLng1(line: List<Pair<Double, Double>>, fromIndex: Int, lat: Double, lng: Double): Double {
        val normalIntersectPoint = normalIntersectPoint(
            line[fromIndex],
            line[fromIndex + 1],
            lat, lng
        )

        return getDistSqBetweenLatLng(
            lat,
            lng,
            normalIntersectPoint[0],
            normalIntersectPoint[1]
        )
    }


    /**
     * @param segment
     * @param fromIndex
     * @param lat
     * @param lng
     * @return dist to line, if normal from point to segment intersects segment, otherwise min distance from point to
     * ends
     */
    fun getDistToSegmentLatLng(segment: DoubleArray, fromIndex: Int, lat: Double, lng: Double): Double {
        return if (Geometry.isNearEdge(segment, fromIndex, lat, lng))
            getDistToLineLatLng(segment, fromIndex, lat, lng)
        else
            min(
                getDistBetweenLatLng(segment, fromIndex, lat, lng),
                getDistBetweenLatLng(segment, fromIndex + 2, lat, lng)
            )
    }


    /**
     * @param segment
     * @param fromIndex
     * @param lat
     * @param lng
     * @return dist to line, if normal from point to segment intersects segment, otherwise min distance from point to
     * ends
     */
    fun getDistToSegmentLatLng1(segment: List<Pair<Double, Double>>, fromIndex: Int, lat: Double, lng: Double): Double {
        return if (Geometry.isNearEdge1(segment, fromIndex, lat, lng))
            getDistToLineLatLng1(segment, fromIndex, lat, lng)
        else
            Math.min(
                getDistBetweenLatLng(
                    segment[fromIndex].first, segment[fromIndex].second, lat, lng
                ),
                getDistBetweenLatLng(
                    segment[fromIndex + 1].first, segment[fromIndex + 1].second, lat, lng
                )
            )
    }


    fun minDistToTrackLatLng(track: DoubleArray, lat: Double, lng: Double): Pair<Int, Double> {
        var minDist = Double.MAX_VALUE
        var dist: Double
        var index: Int = -2
        val end = track.size - 3
        for (i in 0..end step 2) {
            dist = getDistToSegmentLatLng(track, i, lat, lng)
            if (dist < minDist) {
                minDist = dist
                index = i
            }
        }

        return Pair(index / 2, minDist)
    }


    fun minDistToTrackLatLng1(track: List<Pair<Double, Double>>, lat: Double, lng: Double): Pair<Int, Double> {
        var minDist = Double.MAX_VALUE
        var dist: Double
        var index: Int = -1
        for (i in 0 until track.size - 1) {
            dist = getDistToSegmentLatLng1(track, i, lat, lng)
            if (dist < minDist) {
                minDist = dist
                index = i
            }
        }

        return Pair(index, minDist)
    }


}