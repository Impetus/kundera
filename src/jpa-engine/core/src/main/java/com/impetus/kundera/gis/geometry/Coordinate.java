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
 * Geometric Coordinate implementation
 * 
 * @author amresh.singh
 */
public class Coordinate extends com.vividsolutions.jts.geom.Coordinate
{
    /**
     * 
     */
    public Coordinate()
    {
        super();

    }

    /**
     * @param c
     */
    public Coordinate(com.vividsolutions.jts.geom.Coordinate c)
    {
        super(c);

    }

    /**
     * @param x
     * @param y
     * @param z
     */
    public Coordinate(double x, double y, double z)
    {
        super(x, y, z);

    }

    /**
     * @param x
     * @param y
     */
    public Coordinate(double x, double y)
    {
        super(x, y);

    }

}
