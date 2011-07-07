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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author impetus
 * 
 */
public class ClientIdentifier
{

    private String[] node;

    private int port;

    private String keyspace;

    private ClientType clientType;

    private String persistenceUnit;

    public ClientIdentifier(String[] contactNodes, int port, String keyspace, ClientType clientType,
            String persistenceUnit)
    {
        this.node = contactNodes;
        this.port = port;
        this.keyspace = keyspace;
        this.clientType = clientType;
        this.persistenceUnit = persistenceUnit;
    }

    public String[] getNode()
    {
        return node;
    }

    public int getPort()
    {
        return port;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public ClientType getClientType()
    {
        return clientType;
    }

    /**
     * @return the persistenceUnit
     */
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public boolean equals(Object client)
    {
        if (!(client instanceof ClientIdentifier))
        {
            return false;
        }
        else
        {
            ClientIdentifier proxy = (ClientIdentifier) client;
            return proxy.getClientType().equals(this.clientType) && proxy.getKeyspace().equals(this.getKeyspace())
                    && proxy.getPort() == (this.getPort()) && proxy.getNode().equals(this.getNode())
                    && proxy.getPersistenceUnit().equals(this.getPersistenceUnit());
        }

    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
