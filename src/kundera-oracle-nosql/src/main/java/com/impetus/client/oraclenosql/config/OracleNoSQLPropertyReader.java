/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * XML Property reader for OracleNoSQL specific configuration
 * 
 * @author amresh.singh
 */
public class OracleNoSQLPropertyReader extends AbstractPropertyReader implements PropertyReader
{
    /** log instance */
    private static Logger log = LoggerFactory.getLogger(OracleNoSQLPropertyReader.class);

    /** OracleNoSQL schema metadata instance */
    public static OracleNoSQLSchemaMetadata osmd;

    public OracleNoSQLPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        osmd = new OracleNoSQLSchemaMetadata();
    }

    /**
     * Sets Client properties from XML configuration file into
     * {@link OracleNoSQLSchemaMetadata}
     */
    @Override
    protected void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            osmd.setClientProperties(cp);
        }
    }

    /**
     * Holds property related to OracleNoSQL specific configuration file
     * 
     * @author Amresh
     * 
     */
    public class OracleNoSQLSchemaMetadata
    {
        private static final String ORACLE_NOSQL_DATASTORE = "oraclenosql";

        private ClientProperties clientProperties;

        public OracleNoSQLSchemaMetadata()
        {

        }

        /**
         * @return the clientProperties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * @param clientProperties
         *            the clientProperties to set
         */
        private void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }

        /**
         * Returns datastore instance for given {@link ClientProperties} for
         * OracleNoSQL
         * 
         * @return
         */
        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase(ORACLE_NOSQL_DATASTORE))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }
        
        public Properties getConnectionProperties()
        {
            DataStore ds = getDataStore();
            Properties properties = new Properties();
            if (ds != null)
            {
                if (ds.getConnection() != null)
                {
                    properties = ds.getConnection().getProperties();
                    return properties != null ? properties : new Properties();
                }
                if (log.isWarnEnabled())
                {
                    log.warn("No connection properties found, returning none.");
                }
            }
            return properties;
        }

        public List<Server> getConnectionServers()
        {
            DataStore ds = getDataStore();
            List<Server> servers = new ArrayList<Server>();
            if (ds != null && ds.getConnection() != null)
            {
                servers = ds.getConnection().getServers();
                return servers;
            }
            return servers;
        }
    }

}