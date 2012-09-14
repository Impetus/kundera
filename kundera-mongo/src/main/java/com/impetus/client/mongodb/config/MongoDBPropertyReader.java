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
package com.impetus.client.mongodb.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.mongodb.ReadPreference;

/**
 * Mongo Property Reader reads mongo properties from property file
 * {kundera-mongo.properties} and put it into mongo schema metadata.
 * 
 * @author kuldeep.mishra
 * 
 */
public class MongoDBPropertyReader extends AbstractPropertyReader implements PropertyReader
{
    /** log instance */
    private static Log log = LogFactory.getLog(MongoDBPropertyReader.class);

    /** MongoDB schema metadata instance */
    public static MongoDBSchemaMetadata msmd;

    public MongoDBPropertyReader()
    {
        msmd = new MongoDBSchemaMetadata();
    }

    public void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            msmd.setClientProperties(cp);
        }
    }

    public void onProperties(Properties properties)
    {
        if (properties != null)
        {
            log.warn("Use of properties file is Deprecated ,please use xml file instead ");
            msmd = new MongoDBSchemaMetadata(properties.getProperty(MongoDBConstants.CONNECTIONS));
            msmd.setSocketTimeOut(properties.getProperty(MongoDBConstants.SOCKET_TIMEOUT));
            msmd.setReadPreference(properties.getProperty(MongoDBConstants.READ_PREFERENCE));
        }
        else
        {
            log.warn("No property file found in class path, kundera will use default property");
        }
    }

    /**
     * MongoDBSchemaMetadata class holds property related to metadata
     * 
     * @author kuldeep.mishra
     * 
     */
    public class MongoDBSchemaMetadata
    {

        private static final String READ_PREFERENCE_SECONDARY = "secondary";

        private static final String READ_PREFERENCE_PRIMARY = "primary";

        private List<MongoDBConnection> connections;

        private int socketTimeout = 0;

        private ReadPreference preference = ReadPreference.PRIMARY;

        private ClientProperties clientProperties;

        public MongoDBSchemaMetadata()
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

        private MongoDBSchemaMetadata(final String connectionStr)
        {
            String[] tokens = { "host", "port" };
            Map<String, String> hostPort = new HashMap<String, String>();
            // parse connectionStr
            StringTokenizer connections = new StringTokenizer(connectionStr, ",");
            while (connections.hasMoreTokens())
            {
                StringTokenizer connection = new StringTokenizer(connections.nextToken(), ":");
                int count = 0;
                while (connection.hasMoreTokens())
                {
                    hostPort.put(tokens[count++], connection.nextToken());
                }
                addConnection(hostPort.get(tokens[0]), hostPort.get(tokens[1]));
            }
        }

        private void addConnection(String host, String port)
        {
            if (connections == null)
            {
                connections = new ArrayList<MongoDBPropertyReader.MongoDBSchemaMetadata.MongoDBConnection>();
            }
            MongoDBConnection connection = null;
            if (host != null && port != null)
            {
                connection = new MongoDBConnection(host, port);
            }
            else
            {
                // TODO
                return;
            }

            if (connection != null && !connections.contains(connection))
            {
                connections.add(connection);
            }
        }

        /**
         * @return the connections
         */
        public List<MongoDBConnection> getConnections()
        {
            return connections;
        }

        /**
         * @return the timeOut
         */
        public int getSocketTimeOut()
        {
            return socketTimeout;
        }

        /**
         * @param timeOut
         *            the timeOut to set
         */
        private void setSocketTimeOut(String timeOut)
        {
            try
            {
                if (timeOut != null)
                {
                    this.socketTimeout = Integer.parseInt(timeOut);
                }
            }
            catch (NumberFormatException nfe)
            {
                log.warn("socket timeout should be numeric");
            }
        }

        /**
         * @return the preference
         */
        public ReadPreference getReadPreference()
        {
            return preference;
        }

        /**
         * @param preference
         *            the preference to set
         */
        private void setReadPreference(String preference)
        {
            if (preference != null)
            {
                if (preference.equalsIgnoreCase(READ_PREFERENCE_PRIMARY))
                {
                    this.preference = ReadPreference.PRIMARY;
                }
                else if (preference.equalsIgnoreCase(READ_PREFERENCE_SECONDARY))
                {
                    this.preference = ReadPreference.SECONDARY;
                }
                else
                {
                    log.warn("Incorrect Read Preference specified. Only primary/ secondary allowed");
                }
            }
        }

        /**
         * MongoDBCOnnection class
         * 
         * @author kuldeep.mishra
         * 
         */
        public class MongoDBConnection
        {
            private String host;

            private String port;

            MongoDBConnection(String host, String port)
            {
                this.host = host;
                this.port = port;
            }

            /**
             * @return the host
             */
            public String getHost()
            {
                return host;
            }

            /**
             * @return the port
             */
            public String getPort()
            {
                return port;
            }

            @Override
            public boolean equals(Object o)
            {
                if (o == null)
                {
                    return false;
                }

                if (!(o instanceof MongoDBConnection))
                {
                    return false;
                }

                MongoDBConnection connection = (MongoDBConnection) o;
                if (connection.host != null && connection.host.equals(this.host) && connection.port != null
                        && connection.port.equals(this.port))
                {
                    return true;
                }
                return false;
            }

            @Override
            public int hashCode()
            {
                return HashCodeBuilder.reflectionHashCode(this.host + this.port);
            }

            @Override
            public String toString()
            {
                StringBuilder builder = new StringBuilder(host);
                builder.append(":");
                builder.append(port);
                return builder.toString();
            }
        }

        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().equalsIgnoreCase("mongo"))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
