/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.core;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.dao.utils.JsonUtil;
import com.impetus.dao.utils.PropertyReader;
import com.impetus.kundera.KunderaException;

/**
 * The Class PersistenceService.
 */
public class PersistenceService
{

    /** The logger. */
    private static Logger LOGGER = LoggerFactory.getLogger(PersistenceService.class);

    /** The client properties. */
    private static Map<?, Map<String, String>> clientProperties = new HashMap<>();

    /** The entity configurations. */
    private static Map<String, Map<String, String>> entityConfigurations = Collections
            .synchronizedMap(new HashMap<String, Map<String, String>>());

    /**
     * Gets the em.
     *
     * @param emf
     *            the emf
     * @param propertiesPath
     *            the properties path
     * @param clazzName
     *            the clazz name
     * @return the em
     */
    public static synchronized EntityManager getEM(EntityManagerFactory emf, final String propertiesPath,
            final String clazzName)
    {
        loadClientProperties(propertiesPath, clazzName);

        if (emf == null)
        {
            try
            {
                emf = Persistence.createEntityManagerFactory("testPU", entityConfigurations.get(clazzName));

            }
            catch (Exception e)
            {
                LOGGER.error("Unable to create Entity Manager Factory. Caused By: ", e);
                throw new KunderaException("Unable to create Entity Manager Factory. Caused By: ", e);
            }
        }

        return emf.createEntityManager();
    }

    /**
     * Load client properties.
     *
     * @param propertiesPath
     *            the properties path
     * @param clazzName
     *            the clazz name
     */
    private static void loadClientProperties(String propertiesPath, String clazzName)
    {

        InputStream inputStream = PropertyReader.class.getClassLoader().getResourceAsStream(propertiesPath);
        clientProperties = JsonUtil.readJson(inputStream, Map.class);

        if (clientProperties != null)
        {
            if (clientProperties.get("all") != null)
            {
                entityConfigurations.put(clazzName, clientProperties.get("all"));
            }
            else
            {

                Iterator iter = clientProperties.keySet().iterator();

                while (iter.hasNext())
                {
                    Object key = iter.next();
                    if (((String) key).indexOf(',') > 0)
                    {
                        StringTokenizer tokenizer = new StringTokenizer((String) key, ",");
                        while (tokenizer.hasMoreElements())
                        {
                            String token = tokenizer.nextToken();
                            entityConfigurations.put(token, clientProperties.get(key));
                        }
                    }
                    else
                    {
                        entityConfigurations.put((String) key, clientProperties.get(key));
                    }
                }
            }
        }

    }

}
