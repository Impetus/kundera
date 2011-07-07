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
/**
 * 
 */
package com.impetus.kundera.loader;

import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.Client;

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
    public static Client getClient(ClientIdentifier clientIdentifier)
    {
        if (clientsNew.containsKey(clientIdentifier))
        {
            return clientsNew.get(clientIdentifier);
        }
        else
        {
            return loadNewProxyInstance(clientIdentifier);
        }
    }

    private static Client loadNewProxyInstance(ClientIdentifier clientIdentifier)
    {
        Client proxy = null;
        try
        {
            if (clientIdentifier.getClientType().equals(ClientType.HBASE))
            {
                proxy = (Client) Class.forName("com.impetus.kundera.hbase.client.HBaseClient").newInstance();
            }
            else if (clientIdentifier.getClientType().equals(ClientType.PELOPS))
            {
                proxy = (Client) Class.forName("com.impetus.kundera.client.PelopsClient").newInstance();
            }
            else if (clientIdentifier.getClientType().equals(ClientType.THRIFT))
            {
                proxy = (Client) Class.forName("com.impetus.kundera.client.ThriftClient").newInstance();
            }
            else if (clientIdentifier.getClientType().equals(ClientType.MONGODB))
            {
                proxy = (Client) Class.forName("com.impetus.kundera.client.MongoDBClient").newInstance();
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

        if (proxy != null)
        {
            proxy.setContactNodes(clientIdentifier.getNode());
            proxy.setDefaultPort(clientIdentifier.getPort());
            proxy.setKeySpace(clientIdentifier.getKeyspace());
            clientsNew.put(clientIdentifier, proxy);
        }
        else
        {
            throw new ClientResolverException("No client configured:");
        }
        return proxy;
    }
}
