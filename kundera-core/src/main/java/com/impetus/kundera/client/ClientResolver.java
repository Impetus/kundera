/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Resolver class for client. It instantiates client factory and discover specific client.
 * 
 * @author vivek.mishra
 */
public final class ClientResolver
{

    /** The client factories. */
    static Map<String, ClientFactory> clientFactories = new ConcurrentHashMap<String, ClientFactory>();

    /** logger instance. */
    private static final Logger logger = LoggerFactory.getLogger(ClientResolver.class);

    /**
     * Gets the client.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the client
     */
    public static Client discoverClient(String persistenceUnit)
    {
        logger.info("Returning client instance for:" + persistenceUnit);
        return clientFactories.get(persistenceUnit).getClientInstance();
    }

    /**
     * Gets the client factory.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the client factory
     */
    public static ClientFactory getClientFactory(String persistenceUnit)
    {
        ClientFactory clientFactory = clientFactories.get(persistenceUnit);

        if (clientFactory != null)
            return clientFactory;

        logger.info("Initializing client factory for: " + persistenceUnit);
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String kunderaClientName = (String) persistenceUnitMetadata.getProperties().get(
                PersistenceProperties.KUNDERA_CLIENT);
        ClientType clientType = ClientType.getValue(kunderaClientName.toUpperCase());

        try
        {
            if (clientType.equals(ClientType.HBASE))
            {
                clientFactory = (ClientFactory) Class.forName("com.impetus.client.hbase.HBaseClientFactory")
                        .newInstance();
            }
            else if (clientType.equals(ClientType.PELOPS))
            {
                clientFactory = (ClientFactory) Class
                        .forName("com.impetus.client.cassandra.pelops.PelopsClientFactory").newInstance();
            }
            else if (clientType.equals(ClientType.THRIFT))
            {
                clientFactory = (ClientFactory) Class
                        .forName("com.impetus.client.cassandra.thrift.ThriftClientFactory").newInstance();
            }
            else if (clientType.equals(ClientType.MONGODB))
            {
                clientFactory = (ClientFactory) Class.forName("com.impetus.client.mongodb.MongoDBClientFactory")
                        .newInstance();
            }
            else if (clientType.equals(ClientType.RDBMS))
            {
                clientFactory = (ClientFactory) Class.forName("com.impetus.client.rdbms.RDBMSClientFactory")
                        .newInstance();
            }
        }
        catch (InstantiationException e)
        {
            logger.error("Error while initializing client factory, Caused b: " + e.getMessage());
            throw new ClientResolverException("Couldn't instantiate class", e);
        }
        catch (IllegalAccessException e)
        {
            logger.error("Error while initializing client factory, Caused b: " + e.getMessage());
            throw new ClientResolverException(e);
        }
        catch (ClassNotFoundException e)
        {
            logger.error("Error while initializing client factory, Caused b: " + e.getMessage());
            throw new ClientResolverException(e);
        }

        if (clientFactory == null)
        {
            logger.error("Client Factory Not Configured For Specified Client Type : ");
            throw new ClientResolverException("Client Factory Not Configured For Specified Client Type.");
        }

        clientFactories.put(persistenceUnit, clientFactory);

        logger.info("Finishing factory initialization");
        return clientFactory;
    }
}
