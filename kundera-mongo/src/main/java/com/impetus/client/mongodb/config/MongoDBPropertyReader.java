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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.ReadPreference;

/**
 * Mongo Property Reader reads mongo properties from property file
 * {kundera-mongo.properties} and put it into mongo schema metadata.
 * 
 * @author kuldeep.mishra
 * 
 */
public class MongoDBPropertyReader implements PropertyReader
{

    /** log instance */
    private static Log log = LogFactory.getLog(MongoDBPropertyReader.class);

    /** MongoDB schema metadata instance */
    public static MongoDBSchemaMetadata msmd;

    public MongoDBPropertyReader()
    {
        // msmd = new MongoDBSchemaMetadata();
    }

    @Override
    public void read(String pu)
    {
        Properties properties = new Properties();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String propertyName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

        InputStream inStream = propertyName != null ? Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyName) : null;
        if (inStream != null)
        {
            try
            {
                properties.load(inStream);
                msmd = new MongoDBSchemaMetadata(properties.getProperty(Constants.CONNECTIONS));
                msmd.setSocketTimeOut(properties.getProperty(Constants.SOCKET_TIMEOUT));
                msmd.setReadPreference(properties.getProperty(Constants.READ_PREFERENCE));
            }
            catch (IOException e)
            {
                log.warn("error in loading properties , caused by :" + e.getMessage());
                throw new KunderaException(e);
            }
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

        public MongoDBSchemaMetadata()
        {

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
    }
}
