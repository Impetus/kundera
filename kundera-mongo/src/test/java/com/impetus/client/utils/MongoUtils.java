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
package com.impetus.client.utils;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.mongodb.DB;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoUtils
{

    private static final Logger logger = LoggerFactory.getLogger(MongoUtils.class);

    /**
     * 
     */
    public static void dropDatabase(EntityManagerFactory emf, String pu)
    {
        EntityManager em = null;
        Map<String, Client> clients = null;
        MongoDBClient client = null;
        if (emf != null)
            em = emf.createEntityManager();

        if (em != null)
            clients = (Map<String, Client>) em.getDelegate();
        if (clients != null)
            client = (MongoDBClient) clients.get(pu);
        if (client != null)
        {
            try
            {
                Field db = client.getClass().getDeclaredField("mongoDb");
                if (!db.isAccessible())
                {
                    db.setAccessible(true);
                }
                DB mongoDB = (DB) db.get(client);
                mongoDB.dropDatabase();
            }
            catch (SecurityException e)
            {
                logger.error("Error while truncating db",e);
            }

            catch (NoSuchFieldException e)
            {
                logger.error("Error while truncating db",e);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Error while truncating db",e);
            }
            catch (IllegalAccessException e)
            {
                logger.error("Error while truncating db",e);
            }
        }

    }
}
