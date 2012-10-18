/**
 * Copyright 2012 Impetus Infotech.
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

/**
 * Geometric Triangle implementation
 * 
 * @author amresh.singh
 */
public class Triangle extends com.vividsolutions.jts.geom.Triangle
{

    public Triangle(double x1, double y1, double x2, double y2, double x3, double y3)
    {
        super(new Coordinate(x1, y1), new Coordinate(x2, y2), new Coordinate(x3, y3));
    }

    /**
     * @param p0
     * @param p1
     * @param p2
     */
    public Triangle(Coordinate p0, Coordinate p1, Coordinate p2)
    {
        super(p0, p1, p2);
    }

}
