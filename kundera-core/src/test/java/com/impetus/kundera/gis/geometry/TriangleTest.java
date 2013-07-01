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

/**
 * @author vivek.mishra
 * 
 * junit for {@link Triangle}.
 *
 */
public class TriangleTest
{

    @Test
    public void test()
    {
        Coordinate p0 = new Coordinate(34.2d,34.4d);
        Coordinate p1 = new Coordinate(31.2d,32.4d);
        Coordinate p2 = new Coordinate(36.2d,35.4d);
        Triangle triangle = new Triangle(p0, p1, p2);
        
        Assert.assertNotNull(triangle.inCentre());
        Assert.assertNotNull(triangle.centroid(p0, p1, p2));
        
        triangle = new Triangle(2d, 4d, 15d, 23d, 23d, 32d);
        Assert.assertNotNull(triangle);
    }

}
