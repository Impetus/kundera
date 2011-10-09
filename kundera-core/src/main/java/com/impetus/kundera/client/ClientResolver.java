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

import com.impetus.kundera.loader.GenericClientLoader;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author impetus
 */
public final class ClientResolver
{

    static Map<String, GenericClientLoader> clientLoaders = new ConcurrentHashMap<String, GenericClientLoader>();

    /**
     * 
     * @param clientIdentifier
     * @return
     */
    public static Client getClient(String persistenceUnit)
    {
        return clientLoaders.get(persistenceUnit).getClientInstance();
    }

    // TODO To move this method to client dicoverer
    public static GenericClientLoader getClientLoader(String persistenceUnit)
    {
        GenericClientLoader loader = clientLoaders.get(persistenceUnit);

        if (loader != null)
            return loader;

        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.getInstance().getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String kunderaClientName = (String) persistenceUnitMetadata.getProperties().get("kundera.client");
        ClientType clientType = ClientType.getValue(kunderaClientName.toUpperCase());

        try
        {
            if (clientType.equals(ClientType.HBASE))
            {
                loader = (GenericClientLoader) Class.forName("com.impetus.client.hbase.HBaseClientLoader")
                        .newInstance();
            }
            else if (clientType.equals(ClientType.PELOPS))
            {
                loader = (GenericClientLoader) Class.forName("com.impetus.client.cassandra.pelops.PelopsClientLoader")
                        .newInstance();
            }
            else if (clientType.equals(ClientType.THRIFT))
            {
                loader = (GenericClientLoader) Class.forName("com.impetus.client.cassandra.thrift.ThriftClientLoader")
                        .newInstance();
            }
            else if (clientType.equals(ClientType.MONGODB))
            {
                loader = (GenericClientLoader) Class.forName("com.impetus.client.mongodb.MongoDBClientLoader")
                        .newInstance();
            }
        }
        catch (InstantiationException e)
        {
            throw new ClientResolverException(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            throw new ClientResolverException(e.getMessage());
        }
        catch (ClassNotFoundException e)
        {
            throw new ClientResolverException(e.getMessage());
        }

        if (loader == null)
        {
            throw new ClientResolverException("Client Loader Not Configured For Specified Client Type.");
        }

        clientLoaders.put(persistenceUnit, loader);

        return loader;
    }
}
