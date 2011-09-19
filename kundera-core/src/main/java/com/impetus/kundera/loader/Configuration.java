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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.apache.log4j.Logger;

import com.impetus.kundera.Client;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.cache.NonOperationalCacheProvider;
import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

// TODO: Auto-generated Javadoc
/**
 * Configuration loader.
 * 
 * @author impetus
 * 
 */
public class Configuration
{

    /** The emf map. */
    private static Map<String, EntityManagerFactory> emfMap = new HashMap<String, EntityManagerFactory>();

    /** The em map. */
    private static Map<ClientIdentifier, EntityManager> emMap = new HashMap<ClientIdentifier, EntityManager>();

    /** Full path of Server config file */
    private String serverConfig;

    /** The node. */
    private String node;

    /** The port. */
    private String port;

    /** The keyspace. */
    private String keyspace;

    /** The identifier. */
    private ClientIdentifier identifier;

    /** The logger. */
    private static Logger logger = Logger.getLogger(Configuration.class);

    /**
     * Gets the entity manager.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the entity manager
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public EntityManager getEntityManager(String persistenceUnit)
    {
        EntityManager em;
        EntityManagerFactory emf;

        if (emfMap.get(persistenceUnit) == null)
        {
            emf = (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(persistenceUnit);
            emfMap.put(persistenceUnit, emf);
        }
        else
        {
            emf = emfMap.get(persistenceUnit);
        }

        try
        {
            Map propMap = (Map) PropertyAccessorHelper.getObject(emf, emf.getClass().getDeclaredField("props"));

            Properties props = new Properties();
            props.putAll(propMap);

            String client = props.getProperty("kundera.client"); // Kundera
                                                                 // Client

            // Server configuration path
            String serverConfig = props.getProperty("server.config");
            if (serverConfig != null)
            {
                serverConfig = "file:///" + props.getProperty("server.config");
                System.setProperty("cassandra.config", serverConfig);
            }
            node = props.getProperty("kundera.nodes"); // Node on which
                                                       // datastore is running
            port = props.getProperty("kundera.port");
            keyspace = props.getProperty("kundera.keyspace");

            String resourceName = "net.sf.ehcache.configurationResourceName";
            setField(emf, emf.getClass().getDeclaredField("cacheProvider"),
                    initSecondLevelCache(props.getProperty("kundera.cache.provider_class"), resourceName));

            // Client Type (Pelops/ Thrift/ HBase/ MongoDB etc)
            ClientType clientType = ClientType.getValue(client.toUpperCase());

            createIdentifier(clientType, persistenceUnit);

            if (!emMap.containsKey(identifier) || emMap.get(identifier) == null)
            {
                em = emf.createEntityManager();
                emMap.put(identifier, em);
            }
            else
            {
                em = emMap.get(identifier);
            }

            // Set and connect to client
            setClient(em, persistenceUnit);
            logger.info("Kundera Client for Persistence Unit " + persistenceUnit + " is: " + client);

        }
        catch (SecurityException e)
        {
            throw new PersistenceException(e.getMessage());
        }
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            throw new PersistenceException(e.getMessage());
        }
        return em;
    }

    /**
     * Returns entityManager.
     * 
     * @param clientType
     *            client type.
     * @return em entityManager.
     */
    public EntityManager getEntityManager(ClientType clientType)
    {
        return emMap.get(clientType);
    }

    /**
     * Invoked on end of application.
     */
    public void destroy()
    {
        for (EntityManager em : emMap.values())
        {
            if (em.isOpen())
            {
                em.flush();
                em.clear();
                em.close();
                em = null;
            }
        }
        for (EntityManagerFactory emf : emfMap.values())
        {
            emf.close();
            emf = null;
        }
    }

    /**
     * Set client to entity manager.
     * 
     * @param em
     *            the em
     * @param clientType
     *            the client type
     * @param persistenceUnit
     *            the persistence unit
     */
    public void setClient(EntityManager em, String persistenceUnit)
    {
        try
        {
            setField(em, em.getClass().getDeclaredField("client"), getClient(persistenceUnit));
        }
        catch (NoSuchFieldException e)
        {
            throw new PersistenceException(e.getMessage());
        }
    }

    /**
     * Sets the field.
     * 
     * @param obj
     *            the obj
     * @param f
     *            the f
     * @param value
     *            the value
     */
    private void setField(Object obj, Field f, Object value)
    {
        try
        {
            PropertyAccessorHelper.set(obj, f, value);
        }
        catch (SecurityException e)
        {
            throw new PersistenceException(e.getMessage());
        }
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e.getMessage());
        }
    }

    /**
     * Gets the client.
     * 
     * @param clientType
     *            the client type
     * @param persistenceUnit
     *            the persistence unit
     * @return the client
     */
    public Client getClient(String persistenceUnit)
    {
        Client client = ClientResolver.getClient(identifier);
        client.connect();
        return client;
    }

    /**
     * Creates the identifier.
     * 
     * @param clientType
     *            the client type
     * @param persistenceUnit
     *            the persistence unit
     */
    private void createIdentifier(ClientType clientType, String persistenceUnit)
    {
        identifier = new ClientIdentifier(new String[] { node }, Integer.valueOf(port), keyspace, clientType,
                persistenceUnit);
    }

    /**
     * Inits the second level cache.
     * 
     * @param cacheProviderClassName
     *            the cache provider class name
     * @param classResourceName
     *            the class resource name
     * @return the cache provider
     */
    @SuppressWarnings("unchecked")
    private CacheProvider initSecondLevelCache(String cacheProviderClassName, String classResourceName)
    {
        // String cacheProviderClassName = (String)
        // props.get("kundera.cache.provider_class");
        CacheProvider cacheProvider = null;
        if (cacheProviderClassName != null)
        {
            try
            {
                Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
                cacheProvider = cacheProviderClass.newInstance();
                cacheProvider.init(classResourceName);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        if (cacheProvider == null)
        {
            cacheProvider = new NonOperationalCacheProvider();
        }
        return cacheProvider;
    }
}
