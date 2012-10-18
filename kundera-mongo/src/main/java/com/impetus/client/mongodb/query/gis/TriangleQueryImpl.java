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

import com.impetus.kundera.gis.geometry.Triangle;
import com.mongodb.BasicDBObject;

/**
 * Provides methods for geospatial queries specific to triangle shape 
 * @author amresh.singh
 */
public class TriangleQueryImpl implements GeospatialQuery
{

    @Override
    public BasicDBObject createGeospatialQuery(String geolocationColumnName, Object shape, BasicDBObject query)
    {
        List triangleList = new ArrayList();
        
        Triangle triangle = (Triangle) shape;
        
        triangleList.add(new double[] {triangle.p0.x, triangle.p0.y}); //A
        triangleList.add(new double[]{triangle.p1.x, triangle.p1.y}); // B
        triangleList.add(new double[]{triangle.p2.x, triangle.p2.y}); // C
        
        if(query == null) query = new BasicDBObject();       
        
        query.put(geolocationColumnName, new BasicDBObject("$within", new BasicDBObject("$polygon", triangleList)));  
        
        return query;
    }
    

}
