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

import com.impetus.kundera.gis.SurfaceType;
import com.impetus.kundera.gis.geometry.Point;
import com.mongodb.BasicDBObject;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
public class NearQueryImpl implements GeospatialQuery
{
    
    @Override
    public BasicDBObject createGeospatialQuery(String geolocationColumnName, Object shape, BasicDBObject query)
    {
        
        if(query == null)
        {
            query = new BasicDBObject();
        }       
        
        //Set point in query which is involved in near search
        if(shape != null && shape.getClass().isAssignableFrom(Point.class))
        {
            Point point = (Point) shape;
            BasicDBObject filter = (BasicDBObject)query.get(geolocationColumnName); 
                
            if(filter == null) filter = new BasicDBObject(); 
            
            if(point.getSurfaceType().equals(SurfaceType.SPHERICAL)) 
            {
                filter.put("$nearSphere", new double[] {point.getX(), point.getY()});
            }
            else
            {
                filter.put("$near", new double[] {point.getX(), point.getY()});
            }           
            
            query.put(geolocationColumnName, filter);
        }
        
        //Set maximum distance from the given point
        if(shape != null && (shape.getClass().isAssignableFrom(Double.class) ||shape.getClass().isAssignableFrom(double.class)))
        {
            Double maxDistance = (Double) shape;
            
            BasicDBObject filter = (BasicDBObject)query.get(geolocationColumnName); 
            
            if(filter == null) filter = new BasicDBObject();   
            
            filter.put("$maxDistance", maxDistance);
           
            query.put(geolocationColumnName, filter);
        }
        
        return query;
    }      

}
