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
package com.impetus.kundera.loader;

import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.Client;
import com.impetus.kundera.startup.Loader;
import com.impetus.kundera.startup.model.KunderaMetadata;

/**
 * @author impetus
 */
public final class ClientResolver
{

    static Map<ClientIdentifier, Client> clientsNew = new HashMap<ClientIdentifier, Client>();

    /**
     * 
     * @param clientIdentifier
     * @return
     */
    public static Client getClient(String persistenceUnit, ClientIdentifier clientIdentifier)
    {

        Client client = getClientInstance(persistenceUnit);

        client.setContactNodes(clientIdentifier.getNode());
        client.setDefaultPort(clientIdentifier.getPort());
        client.setSchema(clientIdentifier.getKeyspace());
        clientsNew.put(clientIdentifier, client);

        return client;
    }

    public static Client getClientInstance(String persistenceUnit)
    {
        Client client = null;
        try
        {

            client = (Client) Class.forName(
                    KunderaMetadata.getInstance().getClientMetadata(persistenceUnit).getClientImplementor())
                    .newInstance();
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

        if (client == null)
        {
            throw new ClientResolverException("Client Not Configured For Specified Client Type.");
        }

        return client;
    }

    // TODO To move this method to client dicoverer
    public static Loader getClientLoader(ClientType clientType)
    {
        Loader loader = null;
        try
        {
            if (clientType.equals(ClientType.HBASE))
            {
                loader = (Loader) Class.forName("com.impetus.client.hbase.HBaseClientLoader").newInstance();
            }
            else if (clientType.equals(ClientType.PELOPS))
            {
                loader = (Loader) Class.forName("com.impetus.client.cassandra.pelops.PelopsClientLoader").newInstance();
            }
            else if (clientType.equals(ClientType.THRIFT))
            {
                loader = (Loader) Class.forName("com.impetus.client.cassandra.thrift.ThriftClientLoader").newInstance();
            }
            else if (clientType.equals(ClientType.MONGODB))
            {
                loader = (Loader) Class.forName("com.impetus.client.mongodb.MongoDBClientLoader").newInstance();
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

        return loader;
    }
}
