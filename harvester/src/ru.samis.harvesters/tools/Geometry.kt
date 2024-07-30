package ru.samis.harvesters.tools

object Geometry {

    fun getRadians(degrees: Double): Double {
        return degrees * Math.PI / 180
    }

    fun getAngleToLine(line: DoubleArray): Double {
        val A: Double
        val B: Double
        val x1: Double
        val x2: Double
        val y1: Double
        val y2: Double
        val cosa: Double
        val sina: Double
        // lat *= LATITUDE_TO_M;
        // lng *= LONGITUDE_TO_M;
        x1 = line[0]
        x2 = line[2]
        y1 = line[1]
        y2 = line[3]
        A = 1 / (x2 - x1)
        B = 1 / (y1 - y2)
        cosa = -A / Math.sqrt(A * A + B * B)
        sina = -B / Math.sqrt(A * A + B * B)
        val result = Math.acos(cosa) * 180.0 * Math.signum(sina) / Math.PI

        return result - 90.0 * Math.signum(cosa) * Math.signum(sina)
    }

    /**
     * @param routePoints attay of [x, y, x, y, ...]
     * @param x
     * @param y
     * @return
     */
    private fun getClosestEdgeID(routePoints: DoubleArray, x: Double, y: Double): Int {
        var result = -1
        var minLength = 100000.0
        var d: Double
        val length = routePoints.size - 2
        var i = 0
        while (i < length) {
            d = getDistToSegment(routePoints, i, x, y)
            if (d < minLength) {
                minLength = d
                result = i
            }
            i += 2
        }
        return result / 2
    }

    /**
     * determines if normal from (x,y) to line[fromIndex]-line[fromIndex+1] intersects specified segment
     *
     * @param line
     * @param fromIndex
     * @param x
     * @param y
     * @return
     */
    fun isNearEdge(line: DoubleArray, fromIndex: Int, x: Double, y: Double): Boolean {
        val diff1 = line[fromIndex] - line[fromIndex + 2]
        val diff2 = line[fromIndex + 1] - line[fromIndex + 3]
        val lcos1 = diff1 * (x - line[fromIndex + 2]) + diff2 * (y - line[fromIndex + 3])
        val lcos2 = diff1 * (x - line[fromIndex]) + diff2 * (y - line[fromIndex + 1])

        return lcos1 >= 0 && lcos2 <= 0
    }


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


    /**
     * determines if normal from (lat,lng) to line[fromIndex]-line[fromIndex+1] intersects specified segment
     *
     * @param line
     * @param fromIndex
     * @param lat
     * @param lng
     * @return
     */
    fun isNearEdge1(line: List<Pair<Double, Double>>, fromIndex: Int, lat: Double, lng: Double): Boolean {
        val diff1 = (line[fromIndex].first - line[fromIndex + 1].first) *
                GeoTools.metersInLatitudeDegree()
        val diff2 = (line[fromIndex].second - line[fromIndex + 1].second) *
                GeoTools.metersInLongitudeDegree(line[fromIndex].first)
        val lcos1 = diff1 * (lat - line[fromIndex + 1].first) * GeoTools.metersInLatitudeDegree() +
                diff2 * (lng - line[fromIndex + 1].second) * GeoTools.metersInLongitudeDegree(line[fromIndex].first)
        val lcos2 = diff1 * (lat - line[fromIndex].first) * GeoTools.metersInLatitudeDegree() +
                diff2 * (lng - line[fromIndex].second) * GeoTools.metersInLongitudeDegree(line[fromIndex].first)

        return lcos1 >= 0 && lcos2 <= 0
    }

    fun getDistBetween(x1: Double, y1: Double, x2: Double, y2: Double): Double {
        return Math.sqrt(getDistSqBetween(x1, y1, x2, y2))
    }

    fun getDistSqBetween(x1: Double, y1: Double, x2: Double, y2: Double): Double {
        return sumSq(x2 - x1, y2 - y1)
    }


    fun getDistBetween(point: DoubleArray, fromIndex: Int, x2: Double, y2: Double): Double {
        return Math.sqrt(getDistSqBetween(point, fromIndex, x2, y2))
    }

    fun getDistSqBetween(point: DoubleArray, fromIndex: Int, x2: Double, y2: Double): Double {
        return getDistSqBetween(point[fromIndex], point[fromIndex + 1], x2, y2)
    }

    fun getDistToLine(line: DoubleArray, fromIndex: Int, x: Double, y: Double): Double {
        return Math.sqrt(getDistSqToLine(line, fromIndex, x, y))
    }

    fun getDistSqToLine(line: DoubleArray, fromIndex: Int, x: Double, y: Double): Double {
        val A = 1 / (line[fromIndex + 2] - line[fromIndex])
        val B = 1 / (line[fromIndex + 1] - line[fromIndex + 3])
        val C = A * (x - line[fromIndex]) + B * (y - line[fromIndex + 1])

        return C * C / sumSq(A, B)
    }

    /**
     * @param segment
     * @param fromIndex
     * @param x
     * @param y
     * @return dist to line, if normal from point to segment intersects segment, otherwise min distance from point to
     * ends
     */
    fun getDistToSegment(segment: DoubleArray, fromIndex: Int, x: Double, y: Double): Double {
        return if (isNearEdge(segment, fromIndex, x, y))
            getDistToLine(segment, fromIndex, x, y)
        else
            Math.min(
                getDistBetween(segment, fromIndex, x, y),
                getDistBetween(segment, fromIndex + 2, x, y)
            )
    }


    fun minDistToTrack(track: DoubleArray, x: Double, y: Double): Pair<Int, Double> {
        var minDist = Double.MAX_VALUE
        var dist: Double
        var index: Int = -2
        val end = track.size - 3
        for (i in 0..end step 2) {
            dist = getDistToSegment(track, i, x, y)
            if (dist < minDist) {
                minDist = dist
                index = i
            }
        }

        return Pair(index / 2, minDist)
    }


    /**
     * @param routeTrack
     * @param x
     * @param y
     * @return mileage to point lies on route track
     */
    fun getFixedPointMileage(routeTrack: DoubleArray, x: Double, y: Double): Double {
        val edgeIndex = getClosestEdgeID(routeTrack, x, y)
        return getFixedPointMileage(routeTrack, edgeIndex, x, y)
    }

    /**
     * @param routeTrack
     * @param pointEdgeIndex
     * @param x
     * @param y
     * @return mileage from track beginning to point lies on it edge with pointEdgeIndex
     */
    fun getFixedPointMileage(
        routeTrack: DoubleArray, pointEdgeIndex: Int, x: Double,
        y: Double
    ): Double {
        var result = calcRouteMileageToEdge(routeTrack, pointEdgeIndex)
        result += getDistBetween(routeTrack, 2 * pointEdgeIndex, x, y)
        return result
    }



    fun getAnyPointMileage(routeTrack: DoubleArray, x: Double, y: Double): Double {
        val edgeIndex = getClosestEdgeID(routeTrack, x, y)
        return getAnyPointMileage(routeTrack, edgeIndex, x, y)
    }


    fun getPointDistToRoute(routeTrack: DoubleArray, x: Double, y: Double): Double {
        return Math.sqrt(getPointDistSqToRoute(routeTrack, x, y))
    }

    fun getPointDistSqToRoute(routeTrack: DoubleArray, x: Double, y: Double): Double {
        val length = routeTrack.size
        var minDist = getDistSqBetween(x, y, routeTrack[0], routeTrack[1])
        var dist: Double
        var i = 2
        while (i < length) {
            if (isNearEdge(routeTrack, i - 2, x, y)) {
                dist = getDistSqToLine(routeTrack, i - 2, x, y)
            } else {
                dist = getDistSqBetween(x, y, routeTrack[i], routeTrack[i + 1])
            }
            if (dist < minDist) {
                minDist = dist
            }
            i += 2
        }
        return minDist
    }

    /**
     * @param routeTrack
     * @param pointEdgeIndex
     * @param x
     * @param y
     * @return mileage along route track to edgeIndex + mileage to any point (x, y)
     */
    fun getAnyPointMileage(
        routeTrack: DoubleArray, pointEdgeIndex: Int, x: Double,
        y: Double
    ): Double {
        if (isNearEdge(routeTrack, 2 * pointEdgeIndex, x, y)) {
            val pointOnRoute = normalIntersectPoint(routeTrack, 2 * pointEdgeIndex, x, y)
            return getFixedPointMileage(routeTrack, pointEdgeIndex, pointOnRoute[0], pointOnRoute[1])
        } else {
            return if (getDistBetween(routeTrack, 2 * pointEdgeIndex, x, y) < getDistBetween(
                    routeTrack,
                    2 * pointEdgeIndex + 2,
                    x,
                    y
                )
            )

                calcRouteMileageToEdge(routeTrack, pointEdgeIndex)
            else
                calcRouteMileageToEdge(routeTrack, pointEdgeIndex + 1)
        }
    }


    fun calcRouteMileageToEdge(routeTrack: DoubleArray, edgeIndex: Int): Double {
        var edgeIndex = edgeIndex
        var result = 0.0
        edgeIndex *= 2
        var i = 0
        while (i < edgeIndex) {
            result += getDistBetween(
                routeTrack[i], routeTrack[i + 1], routeTrack[i + 2],
                routeTrack[i + 3]
            )
            i += 2
        }
        return result
    }


    /**
     * determines intersect point of normal from (x,y) to line with line
     *
     * @param line
     * @param fromIndex
     * @param x
     * @param y
     * @return
     */
    fun normalIntersectPoint(line: DoubleArray, fromIndex: Int, x: Double, y: Double): DoubleArray {
        return normalIntersectPoint(
            line[fromIndex], line[fromIndex + 1], line[fromIndex + 2],
            line[fromIndex + 3],
            x, y
        )
    }

    fun normalIntersectPoint(
        xLine1: Double, yLine1: Double, xLine2: Double,
        yLine2: Double,
        x3: Double, y3: Double
    ): DoubleArray {
        val A = (yLine2 - yLine1) / (xLine2 - xLine1)
        val B = -A * xLine1 + yLine1
        val C = 1 / A * x3 + y3
        val result = DoubleArray(2)
        result[0] = (C - B) / (A + 1 / A)
        result[1] = A * result[0] + B
        return result
    }


    //corrected 7.06.19
    fun normalIntersectPoint(
        loc1: Pair<Double, Double>, loc2: Pair<Double, Double>,
        lat: Double, lng: Double
    ): DoubleArray {
        val minLat = Math.min(Math.min(loc1.first, loc2.first), lat)
        val minLng = Math.min(Math.min(loc1.second, loc2.second), lng)
        val metersInLongitudeDegree = GeoTools.metersInLongitudeDegree(lat)
        val metersInLatitudeDegree = GeoTools.metersInLatitudeDegree()

        val x0 = (loc1.first - minLat) * metersInLatitudeDegree
        val x1 = (loc2.first - minLat) * metersInLatitudeDegree
        val x2 = (lat - minLat) * metersInLatitudeDegree
        val y0 = (loc1.second - minLng) * metersInLongitudeDegree
        val y1 = (loc2.second - minLng) * metersInLongitudeDegree
        val y2 = (lng - minLng) * metersInLongitudeDegree

        val A = (x0 - x1) / (y1 - y0)
        val result = DoubleArray(2)
        result[0] = (x0 + A * (A * x2 + y0 - y2)) / (A * A + 1)
        result[1] = (A * (result[0] - x2) + y2)
        result[0] /= metersInLatitudeDegree
        result[0] += minLat
        result[1] /= metersInLongitudeDegree
        result[1] += minLng

        return result
    }
}