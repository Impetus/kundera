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

/**
 * @author vivek.mishra
 * 
 * junit for {@link Circle}
 *
 */
public class CircleTest
{

    @Test
    public void test()
    {
        Coordinate coordiates2d = new Coordinate(34.2d,34.4d);
        Circle twoDCircle = new Circle(coordiates2d, 5d);
        twoDCircle.setSurfaceType(SurfaceType.SPHERICAL);
        Assert.assertNotNull(twoDCircle);
        Assert.assertEquals(coordiates2d, twoDCircle.getCentre());
        Assert.assertEquals(5d, twoDCircle.getRadius());
        Assert.assertEquals(SurfaceType.SPHERICAL, twoDCircle.getSurfaceType());

        Coordinate coordiates3d = new Coordinate(34.2d,34.4d,12.3d);
        Circle threeDCircle = new Circle(coordiates3d, 5);
        threeDCircle.setSurfaceType(SurfaceType.SPHERICAL);
        Assert.assertNotNull(threeDCircle);
        Assert.assertEquals(coordiates3d, threeDCircle.getCentre());
        Assert.assertEquals(34.2d,threeDCircle.getCentre().x);
        Assert.assertEquals(34.4d,threeDCircle.getCentre().y);
        Assert.assertEquals(12.3d,threeDCircle.getCentre().z);
        Assert.assertTrue(threeDCircle.getCentre().equals3D(coordiates3d));
        Assert.assertTrue(threeDCircle.getCentre().equals2D(coordiates2d));
        Assert.assertEquals(0d,threeDCircle.getCentre().distance(coordiates2d));
        Assert.assertEquals(5d, threeDCircle.getRadius());
        Assert.assertEquals(SurfaceType.SPHERICAL, threeDCircle.getSurfaceType());

        Circle circle = new Circle(23d,34d,12d);
        circle.setCentre(coordiates2d);
        circle.setRadius(3d);
        Assert.assertNotNull(circle);
        Assert.assertEquals(coordiates2d, circle.getCentre());
        Assert.assertEquals(3d, circle.getRadius());
        
    }

}
