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
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.apache.log4j.Logger;

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

    /** Full path of Server config file */
    private String serverConfig;

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
        EntityManager em = null;
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

            Properties props = new Properties();
            props.putAll(emf.getProperties());

            // Server configuration path
            String serverConfig = props.getProperty("server.config");
            if (serverConfig != null)
            {
                serverConfig = "file:///" + props.getProperty("server.config");
                System.setProperty("cassandra.config", serverConfig);
            }

        }
        catch (SecurityException e)
        {
            throw new PersistenceException(e.getMessage());
        }        
        return emf.createEntityManager();
    }

    /**
     * Invoked on end of application.
     */
    public void destroy()
    {
        // for (EntityManager em : emMap.values())
        // {
        // if (em.isOpen())
        // {
        // em.flush();
        // em.clear();
        // em.close();
        // em = null;
        // }
        // }

        // TODO Discuss

        for (EntityManagerFactory emf : emfMap.values())
        {
            emf.close();
            emf = null;
        }
    }

}
