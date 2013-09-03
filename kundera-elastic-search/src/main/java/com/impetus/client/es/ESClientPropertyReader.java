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
package com.impetus.client.es;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection;
import com.impetus.kundera.configure.PropertyReader;

/**
 * @author vivek.mishra
 *
 */
public class ESClientPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    private ESSchemaMetadata esmd;
    
    public ESClientPropertyReader(Map externalProperties)
    {
        super(externalProperties);
        esmd = new ESSchemaMetadata();
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus.kundera.configure.ClientProperties)
     */
    @Override
    protected void onXml(ClientProperties cp)
    {
        if(cp != null)
        {
            esmd.setClientProperties(cp);
        }
    }

    public Properties getConnectionProperties()
    {
        return esmd.getConnectionProperties();
    }
    
    

    private class ESSchemaMetadata extends AbstractSchemaMetadata
    {
        
        public Properties getConnectionProperties()
        {
            DataStore  ds = getDataStore("elasticsearch");
            Connection connection = ds.getConnection();
            if(connection != null)
            {
                Properties properties = connection.getProperties();
                
                return properties;
            }
                    
            return null;
        }
    }
}
