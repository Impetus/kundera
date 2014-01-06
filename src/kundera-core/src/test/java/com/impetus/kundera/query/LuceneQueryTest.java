/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.query.Person.Day;

/**
 * @author vivek.mishra
 *  Junit test case for invalid lucene query scenarios. Rest are tested via EM and persistence delegator.
 */
public class LuceneQueryTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);        
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void test()
    {
        String query = "Select p from Person p";
        KunderaQuery kunderaQuery = new KunderaQuery();
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery, query);
        queryParser.parse();
        kunderaQuery.postParsingInit();

        LuceneQuery luceneQuery = new LuceneQuery(query,kunderaQuery,null);
        
        try
        {
            luceneQuery.populateEntities(null, null);
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
        
        try
        {
            luceneQuery.recursivelyPopulateEntities(null, null);
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
        
        try
        {
            luceneQuery.getReader();
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
     
        Assert.assertEquals(0,luceneQuery.onExecuteUpdate());
    }
    
    @Test
    public void testOnWhereClause()
    {
        
        Person p1 = new Person();
        p1.setAge(32);
        p1.setDay(Day.TUESDAY);
        p1.setPersonId("p1");
        p1.setSalary(6000.345);
        
        em.persist(p1);
        em.clear();
        
        Person p2 = new Person();
        p2.setAge(24);
        p2.setDay(Day.MONDAY);
        p2.setPersonId("p2");
        p2.setSalary(8000.345);
        
        em.persist(p2);
        em.clear();
        
        String query = "Select p from Person p WHERE p.salary > 500.0";
        Query findQuery = em.createQuery(query, Person.class);
        List<Person> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(2, allPersons.size());
        
              
        query = "Select p from Person p WHERE p.salary >= 500.0";
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(2, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary = 6000.345";
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary <= 7000.345";
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());
        Assert.assertEquals(new Integer(32),allPersons.get(0).getAge());
        
        query = "Select p from Person p WHERE p.salary < 7000.345";
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());
        Assert.assertEquals(new Integer(32),allPersons.get(0).getAge());
        
        query = "Select p from Person p WHERE p.salary < 6000.345"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(0, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary < 6000.345"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(0, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary > -2000"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(2, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary = -200.00"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(0, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary >= -200.00"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(2, allPersons.size());
        
        query = "Select p from Person p WHERE p.salary < -400.00"; 
        findQuery = em.createQuery(query, Person.class);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(0, allPersons.size());
        
       
               
        
    }
    
   
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

}
