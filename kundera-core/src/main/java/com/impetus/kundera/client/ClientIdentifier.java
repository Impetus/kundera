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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * The Class ClientIdentifier.
 *
 * @author impetus
 */
public class ClientIdentifier
{

    /** The node. */
    private String[] node;

    /** The port. */
    private int port;

    /** The keyspace. */
    private String keyspace;

    /** The client type. */
    private ClientType clientType;

    /** The persistence unit. */
    private String persistenceUnit;

    /**
     * Instantiates a new client identifier.
     *
     * @param contactNodes the contact nodes
     * @param port the port
     * @param keyspace the keyspace
     * @param clientType the client type
     * @param persistenceUnit the persistence unit
     */
    public ClientIdentifier(String[] contactNodes, int port, String keyspace, ClientType clientType,
            String persistenceUnit)
    {
        this.node = contactNodes;
        this.port = port;
        this.keyspace = keyspace;
        this.clientType = clientType;
        this.persistenceUnit = persistenceUnit;
    }

    /**
     * Gets the node.
     *
     * @return the node
     */
    public String[] getNode()
    {
        return node;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Gets the keyspace.
     *
     * @return the keyspace
     */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * Gets the client type.
     *
     * @return the client type
     */
    public ClientType getClientType()
    {
        return clientType;
    }

    /**
     * Gets the persistence unit.
     *
     * @return the persistenceUnit
     */
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
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

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
