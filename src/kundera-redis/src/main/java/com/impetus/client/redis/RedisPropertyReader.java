/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

package com.impetus.client.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Property reader responsible for: a) Reads property file (xml or .properties)
 * 
 * @author vivek.mishra
 * 
 */
public class RedisPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** log instance */
    private static Logger log = LoggerFactory.getLogger(RedisPropertyReader.class);

    /** MongoDB schema metadata instance */
    public static RedisSchemaMetadata rsmd;

    public RedisPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        rsmd = new RedisSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus
     * .kundera.configure.ClientProperties)
     */
    @Override
    protected void onXml(ClientProperties cp)
    {

        if (cp != null)
        {
            rsmd.setClientProperties(cp);
        }

    }

    public class RedisSchemaMetadata
    {
        private static final String PORT = "port";

        private static final String HOST = "host";

        private ClientProperties clientProperties;

        private HashMap<String, String> properties = new HashMap<String, String>();

        private String host;

        private String port;

        public RedisSchemaMetadata()
        {

        }

        /**
         * @param clientProperties
         *            the clientProperties to set
         */
        private void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
            properties = initializeProperties();
            this.host = properties.get(HOST);
            this.port = properties.get(PORT);
        }

        public Map<String, String> getProperties()
        {
            properties.remove(HOST);
            properties.remove(PORT);
            return (Map<String, String>) Collections.unmodifiableMap(properties);
        }

        public String getHost()
        {
            return host;
        }

        public String getPort()
        {
            return port;
        }

        public String getPassword()
        {
            return properties.get("requirepass");
        }

        private HashMap<String, String> initializeProperties()
        {
            if (clientProperties != null && clientProperties.getDatastores() != null)
            {
                for (DataStore dataStore : clientProperties.getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase("redis"))
                    {
                        return new HashMap(dataStore.getConnection().getProperties());
                    }
                }
            }

            return null;
        }

    }
}
