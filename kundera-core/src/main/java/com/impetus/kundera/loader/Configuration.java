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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;

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

        long start = System.currentTimeMillis();
        if (emfMap.get(persistenceUnit) == null)
        {
            emf = (EntityManagerFactory) Persistence.createEntityManagerFactory(persistenceUnit);
            emfMap.put(persistenceUnit, emf);
        }
        else
        {
            emf = emfMap.get(persistenceUnit);
        }
        logger.debug("EntityManagerFactory Loaded in >>>\t" + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        em = emf.createEntityManager();
        logger.debug("EntityManager Created in >>>\t" + (System.currentTimeMillis() - start));
        return em;
    }

    /**
     * Invoked on end of application.
     */
    public void destroy()
    {
        // TODO Discuss On Closing EM
        for (EntityManagerFactory emf : emfMap.values())
        {
            emf.close();
            emf = null;
        }
    }

}
