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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import com.impetus.kundera.metadata.entities.EmbeddableEntity;
import com.impetus.kundera.metadata.entities.EmbeddableEntityTwo;
import com.impetus.kundera.metadata.entities.SingularEntityEmbeddable;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.Person.Day;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author vivek.mishra
 *  Junit test case for invalid lucene query scenarios. Rest are tested via EM and persistence delegator.
 */
public class LuceneQueryTest
{
    private static final String LUCENE_DIR_PATH = "./lucene";
    
    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;
    
    Map propertyMap=null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap=new HashMap<String, String>();
        propertyMap.put("index.home.dir","./lucene");       
        emf = Persistence.createEntityManagerFactory(PU,propertyMap);
        em = emf.createEntityManager();

    }

    @Test
    public void test()
    {
        String query = "Select p from Person p";
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();

        LuceneQuery luceneQuery = new LuceneQuery(kunderaQuery,null, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        
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
    
    @Test
    public void testOnUpdate()
    {
        
        Person p1 = new Person();
        p1.setPersonName("vivek");
        p1.setAge(32);
        p1.setDay(Day.TUESDAY);
        p1.setPersonId("p1");
        p1.setSalary(6000.345);
        
        em.persist(p1);
        em.clear();
        
              
        Person p = em.find(Person.class, "p1");
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        p.setAge(12);
        p.setPersonName("newvivek");
                   
       em.merge(p);
       em.clear();
       
      
       Query findQuery = em.createQuery("Select p from Person p WHERE p.personName = vivek");
       List<Person> allPersons = findQuery.getResultList();
       Assert.assertEquals(0, allPersons.size());  
       
       findQuery = em.createQuery("Select p from Person p WHERE p.personName = newvivek");
       allPersons = findQuery.getResultList();
       Assert.assertEquals(new Integer(12), allPersons.get(0).getAge()); 
        
    }
    
    @Test
    public void testEmbeddable()
    {
        
        SingularEntityEmbeddable entity = new SingularEntityEmbeddable();
        entity.setKey(1);
        entity.setName("entity");
        entity.setField("name");
        
        EmbeddableEntity embed1 = new EmbeddableEntity();
        embed1.setField("embeddedField1");
        
        EmbeddableEntityTwo embed2 = new EmbeddableEntityTwo();
        embed2.setField(1f);
        embed2.setName("name");

        entity.setEmbeddableEntity(embed1);
        entity.setEmbeddableEntityTwo(embed2);
        
        em.persist(entity);
        
        SingularEntityEmbeddable p = em.find(SingularEntityEmbeddable.class, 1);
        Assert.assertNotNull(p);
        p.getEmbeddableEntity().setField("embeddedFieldChange");
        
        em.merge(p);
        
        p = em.find(SingularEntityEmbeddable.class, 1);
        Assert.assertEquals("embeddedFieldChange", p.getEmbeddableEntity().getField());
        
    }
    
    @Test
    public void testCollection()
    {
        
        SingularEntityEmbeddable entity = new SingularEntityEmbeddable();
        entity.setKey(1);
        entity.setName("entity");
        entity.setField("name");
        
        EmbeddableEntity embed1 = new EmbeddableEntity();
        embed1.setField("embeddedField1");
        
        EmbeddableEntityTwo embed2 = new EmbeddableEntityTwo();
        embed2.setField(1f);
        embed2.setName("name");

        entity.setEmbeddableEntity(embed1);
        entity.setEmbeddableEntityTwo(embed2);
        
        em.persist(entity);
        
        SingularEntityEmbeddable p = em.find(SingularEntityEmbeddable.class, 1);
        Assert.assertNotNull(p);
        p.getEmbeddableEntity().setField("embeddedFieldChange");
        
        em.merge(p);
        
        p = em.find(SingularEntityEmbeddable.class, 1);
        Assert.assertEquals("embeddedFieldChange", p.getEmbeddableEntity().getField());
        
    }
    
    @Test
    public void likeQueryTest()
    {
        Person p1 = new Person();
        p1.setAge(32);
        p1.setDay(Day.TUESDAY);
        p1.setPersonId("p1");
        p1.setSalary(6000.345);
        p1.setPersonName("vivek");
        em.persist(p1);
        em.clear();
        
        Person p2 = new Person();
        p2.setAge(24);
        p2.setDay(Day.MONDAY);
        p2.setPersonId("p2");
        p2.setSalary(8000.345);
        p2.setPersonName("vivek");
        em.persist(p2);
        em.clear();
        
        Person p3 = new Person();
        p3.setAge(24);
        p3.setDay(Day.MONDAY);
        p3.setPersonId("p3");
        p3.setSalary(8000.345);
        p3.setPersonName("vivek");
        em.persist(p3);
        em.clear();

        String qry = "Select p from Person p where p.personName like :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "vi");
        List<Person> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(3, persons.size());

        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "pragalbh");
        persons = q.getResultList();
        assertEquals(0, persons.size());
        
        Person p = new Person();
        p.setAge(20);
        p.setDay(Day.MONDAY);
        p.setPersonId("4");
        p.setPersonName("pragalbh garg");
        
        Person g = new Person();
        g.setAge(20);
        g.setDay(Day.MONDAY);
        g.setPersonId("5");
        g.setPersonName("karthik prasad");
        
        em.persist(p);
        em.persist(g);
        
        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "garg");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "karthik ");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "%garg");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "karthik%");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from Person p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "%kar");
        persons = q.getResultList();
        assertEquals(0, persons.size());
    }
   
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

}