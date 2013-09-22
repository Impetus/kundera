/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.service;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Configure host name and port and build Hosts array.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public abstract class HostConfiguration
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(HostConfiguration.class);

    /** Delay time for host retry */
    protected int retryDelay = 100;

    /**
     * Persistence unit metadata.
     */
    protected PersistenceUnitMetadata persistenceUnitMetadata;

    /**
     * External configuration passed at the time of emf creation.
     */
    protected Map externalProperties;

    /**
     * Array of hosts.
     */
    protected List<Host> hostsList = new CopyOnWriteArrayList<Host>();

    public HostConfiguration(Map externalProperties, List<Server> servers, String persistenceUnit)
    {
        buildHosts(externalProperties, servers, persistenceUnit);
    }

    /**
     * Build host array.
     * 
     * @param externalProperties
     * @param servers
     * @param persistenceUnit
     */
    private void buildHosts(Map externalProperties, List<Server> servers, String persistenceUnit)
    {
        persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(
                persistenceUnit);
        this.externalProperties = externalProperties;
        String hosts = externalProperties != null ? (String) externalProperties
                .get(PersistenceProperties.KUNDERA_NODES) : null;
        String portAsString = externalProperties != null ? (String) externalProperties
                .get(PersistenceProperties.KUNDERA_PORT) : null;

        if (hosts != null)
        {
            buildHosts(hosts, portAsString,  this.hostsList);
        }
        else if (servers != null && servers.size() >= 1)
        {
            buildHosts(servers, this.hostsList);
        }
        else
        {
            Properties props = persistenceUnitMetadata.getProperties();
            String contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
            String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
            buildHosts(contactNodes, defaultPort,  this.hostsList);
        }
    }

    /**
     * Validate host and port.
     * 
     * @param host
     * @param port
     */
    protected void onValidation(final String host, final String port)
    {
        if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
        {
            logger.error("Host or port should not be null / port should be numeric.");
            throw new IllegalArgumentException("Host or port should not be null / port should be numeric.");
        }
    }

    /**
     * 
     * @return Array of hosts.
     */
    public List<Host> getHosts()
    {
        return hostsList;
    }

    protected abstract void buildHosts(List<Server> servers, List<Host> hostsList);

    protected abstract void buildHosts(String hosts, String portAsString, List<Host> hostsList);

    protected abstract void setConfig(Host host, Properties props, Map puProperties);
}
