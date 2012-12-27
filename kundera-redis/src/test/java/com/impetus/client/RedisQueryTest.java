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

import java.util.List;

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

import com.impetus.client.entities.PersonRedis;
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
        logger.info("On testPopulateEntities");

        EntityManager em = emf.createEntityManager();

        final String originalName = "vivek";

        PersonRedis object = new PersonRedis();
        object.setAge(32);
        object.setPersonId(ROW_KEY);
        object.setPersonName(originalName);

        em.persist(object);

        object.setAge(34);
        object.setPersonId(ROW_KEY + 1);
        object.setPersonName(originalName);

        em.persist(object);

        object.setAge(29);
        object.setPersonId(ROW_KEY + 3);
        object.setPersonName(originalName);

        em.persist(object);

        String findWithOutWhereClause = "Select p from PersonRedis p";
        Query query = em.createQuery(findWithOutWhereClause);
        List<PersonRedis> results = query.getResultList();
        Assert.assertEquals(3, results.size());
        
        String findById = "Select p from PersonRedis p where p.personId=:personId";
        query = em.createQuery(findById);
        query.setParameter("personId", ROW_KEY);
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());
//
//        String findByIdAndAge = "Select p from PersonRedis p where p.personId=:personId AND p.age=:age";
//        query = em.createQuery(findByIdAndAge);
//        query.setParameter("personId", ROW_KEY);
//        query.setParameter("age", 32);
//
//        results = query.getResultList();
//        Assert.assertEquals(1, results.size());
//        Assert.assertEquals(originalName, results.get(0).getPersonName());

        String findAgeByBetween = "Select p from PersonRedis p where p.age between :min AND :max";
        query = em.createQuery(findAgeByBetween);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        String findAgeByGTELTEClause = "Select p from PersonRedis p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findAgeByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // Invalid scenario.
        try
        {
            String invalidDifferentClause = "Select p from PersonRedis p where p.personId=:personId AND p.age >=:age";
            query = em.createQuery(invalidDifferentClause);
            query.setParameter("personId", ROW_KEY);
            query.setParameter("age", 32);
            query.getResultList();
            Assert.fail("Must have thrown query handler exception!");
        }
        
        
        catch (QueryHandlerException qhex)
        {
            Assert.assertNotNull(qhex);
        }
        
        // Delete by query.
        String deleteQuery="Delete from PersonRedis p";
        query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();
        
        Assert.assertEquals(3, updateCount);
        
        // Search all after delete.
        findWithOutWhereClause = "Select p from PersonRedis p";
        query = em.createQuery(findWithOutWhereClause);
        results = query.getResultList();
        Assert.assertNull(results);
        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        emf=null;
    }

}
