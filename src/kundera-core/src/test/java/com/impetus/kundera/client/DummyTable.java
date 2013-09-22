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
 * Class used in test cases as dumy table
 * @author amresh.singh
 *
 */
public class DummyTable
{ 
    
    private Map<Object, Object> records;
    
    /**
     * @return the records
     */
    public Map<Object, Object> getRecords()
    {
        return records;
    }
    
    public Object getRecord(Object pk)
    {
        if(records == null) return null;
        
        return records.get(pk);
    }
    
    /**
     * @param records the records to set
     */
    public void addRecord(Object pk, Object record)
    {
        if(records == null)
        {
            records = new HashMap<Object, Object>();
        }
        records.put(pk, record);
    }
    
    public void removeRecord(Object pk)
    {
        if(records != null)
        {
            records.remove(pk);
        }
    }
    
    public void truncate()
    {
        if(records != null)
        {
            records.clear();
        }
    }
    

}
