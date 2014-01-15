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

import com.impetus.kundera.gis.SurfaceType;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * @author vivek.mishra
 * junit for {@link Point}
 *
 */
public class PointTest
{

    @Test
    public void test()
    {
        Coordinate coordiates2d = new Coordinate(34.2d,34.4d);
        Coordinate[] coordinates = new Coordinate[1];
        coordinates[0]=coordiates2d;
        
        com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Float floatSequence = new com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Float(coordinates, 2);

        GeometryFactory geoFactory = new GeometryFactory(new PrecisionModel(2));
        Point point = new Point(floatSequence,geoFactory);
        
        point.setSRID(2);
        point.setSurfaceType(SurfaceType.FLAT);
        
        Geometry geometry = new com.vividsolutions.jts.geom.Point(floatSequence,geoFactory);
        Assert.assertTrue(point.contains(geometry));
        Assert.assertTrue(point.getSurfaceType() != null);
        Assert.assertEquals(SurfaceType.FLAT,point.getSurfaceType());
        Assert.assertEquals(2,point.getSRID());

        point = new Point(coordiates2d,new PrecisionModel(2),12);
        Assert.assertNotNull(point);
    }

}
