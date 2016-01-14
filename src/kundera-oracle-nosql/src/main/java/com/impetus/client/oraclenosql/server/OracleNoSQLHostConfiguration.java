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

package com.impetus.client.oraclenosql.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.config.OracleNoSQLPropertyReader.OracleNoSQLSchemaMetadata;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;

/**
 * Holds host configuration for cassandra specific settings.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class OracleNoSQLHostConfiguration extends HostConfiguration
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(OracleNoSQLHostConfiguration.class);

    public OracleNoSQLHostConfiguration(Map externalProperties, OracleNoSQLSchemaMetadata csmd, String persistenceUnit,
            final KunderaMetadata kunderaMetadata)
    {
        super(externalProperties, csmd != null ? csmd.getConnectionServers() : new ArrayList<Server>(),
                persistenceUnit, kunderaMetadata);
        
        connectionProperties.putAll(csmd.getConnectionProperties());
        
    }

    protected void buildHosts(List<Server> servers, List<Host> hostsList)
    {
        List<OracleNoSQLHost> oracleNoSQLHosts = new CopyOnWriteArrayList<OracleNoSQLHost>();
        
        for (Server server : servers)
        {
            String host = server.getHost().trim();
            String portAsString = server.getPort().trim();
            onValidation(host, portAsString);
            Properties serverProperties = server.getProperties();
            OracleNoSQLHost oracleNoSQLHost = new OracleNoSQLHost(host, Integer.parseInt(portAsString));
            setConfig(oracleNoSQLHost, null, serverProperties);
            oracleNoSQLHosts.add(oracleNoSQLHost);
            hostsList.add(oracleNoSQLHost);
        }
        
    }

    protected void buildHosts(String hosts, String portAsString, List<Host> hostsList)
    {
        String[] hostVals = hosts.split(",");
        List<OracleNoSQLHost> oracleNoSqlHosts = new CopyOnWriteArrayList<OracleNoSQLHost>();
        for (int x = 0; x < hostVals.length; x++)
        {
            String host = hostVals[x].trim();
            portAsString = portAsString.trim();
            onValidation(host, portAsString);
            int port = Integer.parseInt(portAsString);
            OracleNoSQLHost oracleNoSQLHost = port == OracleNoSQLHost.DEFAULT_PORT ? new OracleNoSQLHost(host)
                    : new OracleNoSQLHost(host, port);
            setConfig(oracleNoSQLHost, persistenceUnitMetadata.getProperties(), externalProperties);
            oracleNoSqlHosts.add(oracleNoSQLHost);
            hostsList.add(oracleNoSQLHost);
        }
    }

    @Override
    protected void setConfig(Host host, Properties props, Map externalProperties)
    {
        OracleNoSQLHost oracleNoSQLHost = (OracleNoSQLHost) host;
        String maxActivePerNode = null;
        String maxIdlePerNode = null;
        String minIdlePerNode = null;
        String maxTotal = null;
        String userName = null;
        String password = null;
    
        if (externalProperties != null)
        {
            connectionProperties.putAll(externalProperties);

            maxActivePerNode = (String) connectionProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            maxIdlePerNode = (String) connectionProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
            minIdlePerNode = (String) connectionProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
            maxTotal = (String) connectionProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
            userName = (String) connectionProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) connectionProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
           
        }

        if (props != null)
        {
            if (maxActivePerNode == null)
            {
                maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE).trim() : null;
            }
            if (maxIdlePerNode == null)
            {
                maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE).trim() : null;
            }
            if (minIdlePerNode == null)
            {
                minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE).trim() : null;
            }
            if (maxTotal == null)
            {
                maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL).trim() : null;
            }

            if (userName == null)
            {
                userName = props.getProperty(PersistenceProperties.KUNDERA_USERNAME);
                password = props.getProperty(PersistenceProperties.KUNDERA_PASSWORD);
            }
        }
        try
        {
 
            oracleNoSQLHost.setUserName(userName);
            oracleNoSQLHost.setPassword(password);

          
        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property couldn't be parsed. Default pool policy would be used");
        }
    }

    /**
     * 
     * @return Host array
     */
    public List<Host> getOracleNoSQLHosts()
    {
        return hostsList;
    }



  

    /**
     * 
     * @param host
     * @param port
     * @return CassandraHosts
     */
    public OracleNoSQLHost getOracleNoSQLHost(String host, int port)
    {
        for (Host oraclenoSqlHost : hostsList)
        {
            if (((OracleNoSQLHost) oraclenoSqlHost).equals(new OracleNoSQLHost(host, port)))
            {
                return (OracleNoSQLHost) oraclenoSqlHost;
            }
        }
        return null;
    }
}