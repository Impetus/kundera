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

    private static final String _3_0_0 = "3.0.0";

    private static final String CQL_VERSION = "cql.version";

    private static final String CASSANDRA = "cassandra";

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyReader.class);

    /** The client name to factory map. */
    private static Map<Datasource, String> clientNameToFactoryMap = new EnumMap<>(Datasource.class);

    static
    {
        clientNameToFactoryMap.put(Datasource.MONGODB, EthConstants.KUNDERA_MONGODB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.RETHINKDB, EthConstants.KUNDERA_RETHINKDB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.CASSANDRA, EthConstants.KUNDERA_CASSANDRA_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.HBASE, EthConstants.KUNDERA_HBASE_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.KUDU, EthConstants.KUNDERA_KUDU_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.REDIS, EthConstants.KUNDERA_REDIS_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.COUCHDB, EthConstants.KUNDERA_COUCHDB_CLIENT_FACTORY);
        clientNameToFactoryMap.put(Datasource.KVSTORE, EthConstants.KUNDERA_ONS_CLIENT_FACTORY);
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

        String dbType = reader.getProperty(EthConstants.DATABASE_TYPE);
        String host = reader.getProperty(EthConstants.DATABASE_HOST);
        String port = reader.getProperty(EthConstants.DATABASE_PORT);
        String dbName = reader.getProperty(EthConstants.DATABASE_NAME);

        propertyNullCheck(dbType, host, port, dbName);

        String username = reader.getProperty(EthConstants.DATABASE_USERNAME);
        String pswd = reader.getProperty(EthConstants.DATABASE_PASSWORD);

        Map<String, String> props = new HashMap<>();

        props.put(EthConstants.KUNDERA_CLIENT_LOOKUP_CLASS, getKunderaClientToLookupClass(dbType));
        props.put(EthConstants.KUNDERA_NODES, host);
        props.put(EthConstants.KUNDERA_PORT, port);
        props.put(EthConstants.KUNDERA_KEYSPACE, dbName);
        props.put(EthConstants.KUNDERA_DIALECT, dbType);

        if (username != null && !username.isEmpty() && pswd != null)
        {
            props.put(EthConstants.KUNDERA_USERNAME, username);
            props.put(EthConstants.KUNDERA_PASSWORD, pswd);
        }

        if (dbType.equalsIgnoreCase(CASSANDRA))
        {
            props.put(CQL_VERSION, _3_0_0);
        }

        boolean schemaAutoGen = Boolean.parseBoolean(reader.getProperty(EthConstants.SCHEMA_AUTO_GENERATE));
        boolean schemaDropExisting = Boolean.parseBoolean(reader.getProperty(EthConstants.SCHEMA_DROP_EXISTING));

        if (schemaAutoGen)
        {
            if (schemaDropExisting)
            {
                props.put(EthConstants.KUNDERA_DDL_AUTO_PREPARE, "create");
            }
            else
            {
                props.put(EthConstants.KUNDERA_DDL_AUTO_PREPARE, "update");
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
            LOGGER.error("Property '" + EthConstants.DATABASE_TYPE + "' can't be null or empty");
            throw new KunderaException("Property '" + EthConstants.DATABASE_TYPE + "' can't be null or empty");
        }

        if (host == null || host.isEmpty())
        {
            LOGGER.error("Property '" + EthConstants.DATABASE_HOST + "' can't be null or empty");
            throw new KunderaException("Property '" + EthConstants.DATABASE_HOST + "' can't be null or empty");
        }

        if (port == null || port.isEmpty())
        {
            LOGGER.error("Property '" + EthConstants.DATABASE_PORT + "' can't be null or empty");
            throw new KunderaException("Property '" + EthConstants.DATABASE_PORT + "' can't be null or empty");
        }

        if (dbName == null || dbName.isEmpty())
        {
            LOGGER.error("Property'" + EthConstants.DATABASE_NAME + "' can't be null or empty");
            throw new KunderaException("Property '" + EthConstants.DATABASE_NAME + "' can't be null or empty");
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
