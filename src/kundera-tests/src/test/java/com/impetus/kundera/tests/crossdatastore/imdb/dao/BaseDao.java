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
package com.impetus.kundera.tests.crossdatastore.imdb.dao;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;

public class BaseDao
{

    EntityManagerFactory emf;

    EntityManager em;

    public EntityManager getEntityManager(String pu)
    {
        if (emf == null)
        {
            Map<String, String> propertyMap = new HashMap<String, String>();

            if (propertyMap.isEmpty())
            {
                propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "");
            }
            Map mapOfExternalProperties = new HashMap<String, Map>();
            mapOfExternalProperties.put("imdbCassandra", propertyMap);
            mapOfExternalProperties.put("piccandra", propertyMap);
            mapOfExternalProperties.put("secIdxAddCassandra", propertyMap);
            
            emf = Persistence.createEntityManagerFactory(pu, mapOfExternalProperties);
            em = emf.createEntityManager();
        }

        if (em == null)
        {
            em = emf.createEntityManager();
        }

        return em;
    }

    public void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }
    }

    public void closeEntityManagerFactory()
    {
        if (emf != null)
        {
            emf.close();
        }
        emf = null;
    }

}
