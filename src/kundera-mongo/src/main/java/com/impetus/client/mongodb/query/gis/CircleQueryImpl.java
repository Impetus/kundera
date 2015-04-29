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
package com.impetus.client.mongodb.query.gis;

import java.util.ArrayList;
import java.util.List;

import com.impetus.kundera.gis.SurfaceType;
import com.impetus.kundera.gis.geometry.Circle;
import com.impetus.kundera.gis.query.GeospatialQuery;
import com.mongodb.BasicDBObject;

/**
 * Provides methods for geospatial queries specific to circle shape
 * 
 * @author amresh.singh
 */
public class CircleQueryImpl implements GeospatialQuery
{
    @Override
    public Object createGeospatialQuery(String geolocationColumnName, Object shape, Object query)
    {
        Circle circle = (Circle) shape;
        List circleList = new ArrayList();

        circleList.add(new double[] { circle.getCentre().x, circle.getCentre().y }); // Centre
                                                                                     // of
                                                                                     // circle
        circleList.add(circle.getRadius()); // Radius

        BasicDBObject q = (BasicDBObject) query;

        if (q == null)
            q = new BasicDBObject();

        if (circle.getSurfaceType().equals(SurfaceType.SPHERICAL))
        {
            q.put(geolocationColumnName, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", circleList)));
        }
        else
        {
            q.put(geolocationColumnName, new BasicDBObject("$geoWithin", new BasicDBObject("$center", circleList)));
        }

        return q;
    }

}
