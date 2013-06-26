/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.client;

import java.util.HashMap;
import java.util.Map;

/**
 * Class acting as dummy database for test cases, kind of mocks database
 * @author amresh.singh
 *
 */
public class DummyDatabase
{
    public static final DummyDatabase INSTANCE = new DummyDatabase();
    
    private Map<String, DummySchema> schemas;   
    
    /**
     * @return the schemas
     */
    public Map<String, DummySchema> getSchemas()
    {
        return schemas;
    }
    
    public DummySchema getSchema(String schemaName)
    {
        if(schemas == null) return null;
        return getSchemas().get(schemaName);
    }

    /**
     * @param schemas the schemas to set
     */
    public void addSchema(String schemaName, DummySchema schema)
    {
        if(schemas == null)
        {
            schemas = new HashMap<String, DummySchema>();
        }
        schemas.put(schemaName, schema);
    }



    public void dropDatabase()
    {
        if(schemas != null)
        {
            for(DummySchema schema : schemas.values())
            {
                schema.dropSchema();
            }
            schemas.clear();
        }
        
    }
}


