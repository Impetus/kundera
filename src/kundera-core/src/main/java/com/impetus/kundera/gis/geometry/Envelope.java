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

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Geometric Envelope (Box) implementation
 * 
 * @author amresh.singh
 */
public class Envelope extends com.vividsolutions.jts.geom.Envelope
{

    /**
     * 
     */
    public Envelope()
    {
    }

    /**
     * @param p
     */
    public Envelope(Coordinate p)
    {
        super(p);

    }

    /**
     * @param env
     */
    public Envelope(com.vividsolutions.jts.geom.Envelope env)
    {
        super(env);

    }

    /**
     * @param p1
     * @param p2
     */
    public Envelope(Coordinate p1, Coordinate p2)
    {
        super(p1, p2);

    }

    /**
     * @param x1
     * @param x2
     * @param y1
     * @param y2
     */
    public Envelope(double x1, double x2, double y1, double y2)
    {
        super(x1, x2, y1, y2);

    }

}
