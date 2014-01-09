/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.gis.geometry;

import junit.framework.Assert;

import org.junit.Test;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * @author vivek.mishra junit for {@link Polygon}
 * 
 */
public class PolygonTest
{

    // @Test
    public void test()
    {
        Coordinate coordiates2d = new Coordinate(34.2d, 34.4d);
        Coordinate coordiates2d1 = new Coordinate(34.2d, 34.4d);

        Coordinate[] coordinates = new Coordinate[2];
        coordinates[0] = coordiates2d;
        coordinates[1] = coordiates2d1;

        com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Float floatSequence = new com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Float(
                coordinates, 1);
        GeometryFactory geoFactory = new GeometryFactory(new PrecisionModel(2));

        com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Double doubleSequence = new com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Double(
                coordinates, 1);

        LinearRing shell = new LinearRing(floatSequence, geoFactory);

        LinearRing[] holes = new LinearRing[1];

        holes[0] = new LinearRing(doubleSequence, geoFactory);

        Polygon polygon = new Polygon(shell, holes, geoFactory);

        Assert.assertNotNull(polygon.getCoordinates());
    }

    @Test
    public void dummyTest()
    {

    }

}
