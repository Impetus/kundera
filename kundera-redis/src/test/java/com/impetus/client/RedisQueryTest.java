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

package com.impetus.client;


import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.query.QueryHandlerException;

/**
 * @author vivek
 *
 */
public class RedisQueryTest
{

    private static final String ROW_KEY = "1";

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisQueryTest.class);

    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
    }

    @Test
    public void testPopulateEntites()
    {
        logger.info("On testInsert");
        EntityManager em = emf.createEntityManager();
        
        String findById="Select p from PersonRedis p where p.personId=:personId";
        Query query = em.createQuery(findById);
        query.setParameter("personId", ROW_KEY);
        query.getResultList();
        
        
        String findByIdAndAge="Select p from PersonRedis p where p.personId=:personId AND p.age=:age";
        query = em.createQuery(findByIdAndAge);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 32);
        query.getResultList();
        
        String findAgeByBetween="Select p from PersonRedis p where p.age between :min AND :max";
        query = em.createQuery(findAgeByBetween);
        query.setParameter("min", 32);
        query.setParameter("max", 35);
        query.getResultList();
        
        String findAgeByGTELTEClause="Select p from PersonRedis p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findAgeByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);
        query.getResultList();

        try
        {
        String invalidDifferentClause="Select p from PersonRedis p where p.personId=:personId AND p.age >=:age";
        query = em.createQuery(invalidDifferentClause);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 32);
        query.getResultList();
        Assert.fail("Must have thrown query handler exception!");
        }catch(QueryHandlerException qhex)
        {
            Assert.assertNotNull(qhex);
        }
    }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
