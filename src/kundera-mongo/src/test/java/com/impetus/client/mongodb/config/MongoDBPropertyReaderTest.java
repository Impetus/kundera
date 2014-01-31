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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.client.mongodb.config.MongoDBPropertyReader.MongoDBSchemaMetadata;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.mongodb.ReadPreference;

/**
 * @author impadmin
 * 
 */
public class MongoDBPropertyReaderTest
{

    private static Logger log = LoggerFactory.getLogger(MongoDBPropertyReaderTest.class);

    private List<Connection> connections = new ArrayList<Connection>();

    private String pu = "mongoTest";

    private MongoDBSchemaMetadata dbSchemaMetadata;

    private int timeOut;

    private ReadPreference readPreference;

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(pu);
        new PersistenceUnitConfiguration(null, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), pu)
                .configure();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.client.mongodb.config.MongoDBPropertyReader#read(java.lang.String)}
     * .
     */
    @Test
    public void testRead()
    {
        PropertyReader reader = new MongoDBPropertyReader(null, ((EntityManagerFactoryImpl) emf)
                .getKunderaMetadataInstance().getApplicationMetadata().getPersistenceUnitMetadata(pu));
        reader.read(pu);
        dbSchemaMetadata = MongoDBPropertyReader.msmd;

        Properties properties = new Properties();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), pu);
        String propertyName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

        InputStream inStream = propertyName != null ? ClassLoader.getSystemResourceAsStream(propertyName) : null;
        if (inStream != null)
        {
            try
            {
                properties.load(inStream);
                String readPreference = properties.getProperty(MongoDBConstants.READ_PREFERENCE);
                if (readPreference != null)
                {
                    if (readPreference.equalsIgnoreCase("primary"))
                    {
                        this.readPreference = ReadPreference.PRIMARY;
                    }
                    else if (readPreference.equalsIgnoreCase("secondary"))
                    {
                        this.readPreference = ReadPreference.SECONDARY;
                    }
                    else
                    {
                        log.warn("Incorrect Read Preference specified. Only primary/ secondary allowed");
                    }
                }
                timeOut = Integer.parseInt(properties.getProperty(MongoDBConstants.SOCKET_TIMEOUT));

                parseConnectionString(properties.getProperty(MongoDBConstants.CONNECTIONS));

                // Assert.assertEquals(timeOut,
                // dbSchemaMetadata.getSocketTimeOut());
                // Assert.assertEquals(this.readPreference,
                // dbSchemaMetadata.getReadPreference());
                // Assert.assertEquals(connections.size(),
                // dbSchemaMetadata.getConnections().size());
            }
            catch (NumberFormatException nfe)
            {
                log.info("time out should be numeric");
            }
            catch (IOException e)
            {
                log.info("property file not found in class path");
            }
        }
    }

    private void parseConnectionString(String connectionStr)
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
        Connection connection = null;
        if (host != null && port != null)
        {
            connection = new Connection(host, port);
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

    private class Connection
    {
        private String host;

        private String port;

        public Connection(String host, String port)
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
    }
}
