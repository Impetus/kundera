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

import com.impetus.kundera.gis.geometry.Circle;
import com.impetus.kundera.gis.geometry.Envelope;
import com.impetus.kundera.gis.geometry.Polygon;
import com.impetus.kundera.gis.geometry.Triangle;
import com.impetus.kundera.gis.query.GeospatialQuery;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * Factory for {@link GeospatialQuery}
 * 
 * @author amresh.singh
 */
public class GeospatialQueryFactory
{
    public static GeospatialQuery getGeospatialQueryImplementor(String operator, Object shape)
    {
        if (operator.equalsIgnoreCase("in"))
        {
            if (shape.getClass().isAssignableFrom(Circle.class))
            {
                return new CircleQueryImpl();
            }
            else if (shape.getClass().isAssignableFrom(Envelope.class))
            {
                return new EnvelopeQueryImpl();
            }
            else if (shape.getClass().isAssignableFrom(Triangle.class))
            {
                return new TriangleQueryImpl();
            }
            else if (shape.getClass().isAssignableFrom(Polygon.class))
            {
                return new PolygonQueryImpl();
            }
            else
            {
                throw new QueryHandlerException("Shape " + shape.getClass() + " is not supported"
                        + " in JPA queries for operator " + operator + " in Kundera currently");
            }
        }
        else if (operator.equals(">") || operator.equals(">=") || operator.equals("<") || operator.equals("<="))
        {            
            return new NearQueryImpl();            
        }
        else
        {
            throw new QueryHandlerException("Shape " + shape.getClass() + " is not supported"
                    + " in JPA queries for operator " + operator + " in Kundera currently");
        }
    }  
    

}
