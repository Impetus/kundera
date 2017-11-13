/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.kundera.blockchain.util;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.blockchain.ethereum.Datasource;

/**
 * The Class KunderaPropertyBuilder.
 */
public class KunderaPropertyBuilder
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyReader.class);

    /** The client name to factory map. */
    private static Map<Datasource, String> clientNameToFactoryMap = new EnumMap<>(Datasource.class);

    static
    {
        clientNameToFactoryMap.put(Datasource.MONGODB, Constants.KUNDERA_MONGODB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.RETHINKDB, Constants.KUNDERA_RETHINKDB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.CASSANDRA, Constants.KUNDERA_CASSANDRA_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.HBASE, Constants.KUNDERA_HBASE_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.KUDU, Constants.KUNDERA_KUDU_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.REDIS, Constants.KUNDERA_REDIS_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.COUCHDB, Constants.KUNDERA_COUCHDB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.KVSTORE, Constants.KUNDERA_ONS_CLIENT_FACTORY);
    }

    /**
     * Instantiates a new kundera property builder.
     */
    private KunderaPropertyBuilder()
    {
    }

    /**
     * Populate persistence unit properties.
     *
     * @param reader
     *            the reader
     * @return the map
     */
    public static Map<String, String> populatePersistenceUnitProperties(PropertyReader reader)
    {

        String dbType = reader.getProperty(Constants.DATABASE_TYPE);
        String host = reader.getProperty(Constants.DATABASE_HOST);
        String port = reader.getProperty(Constants.DATABASE_PORT);
        String dbName = reader.getProperty(Constants.DATABASE_NAME);

        propertyNullCheck(dbType, host, port, dbName);

        Map<String, String> props = new HashMap<>();

        props.put(Constants.KUNDERA_CLIENT_LOOKUP_CLASS, getKunderaClientToLookupClass(dbType));
        props.put(Constants.KUNDERA_NODES, host);
        props.put(Constants.KUNDERA_PORT, port);
        props.put(Constants.KUNDERA_KEYSPACE, dbName);
        props.put(Constants.KUNDERA_DIALECT, dbType);

        boolean schemaAutoGen = Boolean.parseBoolean(reader.getProperty(Constants.SCHEMA_AUTO_GENERATE));
        boolean schemaDropExisting = Boolean.parseBoolean(reader.getProperty(Constants.SCHEMA_DROP_EXISTING));

        if (schemaAutoGen)
        {
            if (schemaDropExisting)
            {
                props.put(Constants.KUNDERA_DDL_AUTO_PREPARE, "create");
            }
            else
            {
                props.put(Constants.KUNDERA_DDL_AUTO_PREPARE, "update");
            }
        }
        LOGGER.info("Kundera properties : " + props);
        return props;
    }

    /**
     * Property null check.
     *
     * @param dbType
     *            the db type
     * @param host
     *            the host
     * @param port
     *            the port
     * @param dbName
     *            the db name
     */
    private static void propertyNullCheck(String dbType, String host, String port, String dbName)
    {

        if (dbType == null || dbType.isEmpty())
        {
            LOGGER.error("Property '" + Constants.DATABASE_TYPE + "' can't be null or empty");
            throw new KunderaException("Property '" + Constants.DATABASE_TYPE + "' can't be null or empty");
        }

        if (host == null || host.isEmpty())
        {
            LOGGER.error("Property '" + Constants.DATABASE_HOST + "' can't be null or empty");
            throw new KunderaException("Property '" + Constants.DATABASE_HOST + "' can't be null or empty");
        }

        if (port == null || port.isEmpty())
        {
            LOGGER.error("Property '" + Constants.DATABASE_PORT + "' can't be null or empty");
            throw new KunderaException("Property '" + Constants.DATABASE_PORT + "' can't be null or empty");
        }

        if (dbName == null || dbName.isEmpty())
        {
            LOGGER.error("Property'" + Constants.DATABASE_NAME + "' can't be null or empty");
            throw new KunderaException("Property '" + Constants.DATABASE_NAME + "' can't be null or empty");
        }

    }

    /**
     * Gets the kundera client to lookup class.
     *
     * @param client
     *            the client
     * @return the kundera client to lookup class
     */
    private static String getKunderaClientToLookupClass(String client)
    {

        Datasource datasource;
        try
        {
            datasource = Datasource.valueOf(client.toUpperCase());
        }
        catch (IllegalArgumentException ex)
        {
            LOGGER.error(client + " is not supported!", ex);
            throw new KunderaException(client + " is not supported!", ex);
        }

        return clientNameToFactoryMap.get(datasource);
    }

}
