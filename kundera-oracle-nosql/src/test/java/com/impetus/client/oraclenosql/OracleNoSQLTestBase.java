/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import com.impetus.client.oraclenosql.entities.PersonKVStore;

/**
 * Base class for all test cases 
 * @author amresh.singh
 */
public class OracleNoSQLTestBase
{
    
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    protected void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("twikvstore");
        em = emf.createEntityManager();
    }


    protected void tearDown()
    {
        em.close();
        emf.close();
    }
    
    protected void clearEm()
    {
        em.clear();
    }
    
    
    protected void persist(Object entity)
    {        
        em.persist(entity);
    }
  

    protected Object find(Class<?> entityClass, Object id)
    {
        return em.find(entityClass, id);
    }

    protected void update(Object entity)
    {
        em.merge(entity);
    }   

    protected void delete(Object entity)
    {
        em.remove(entity);
    }

}
