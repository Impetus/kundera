/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

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

import com.impetus.client.crud.PersonMongoNameAnnotation.Day;
import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * Test case to perform check entity name annotation support
 * select)
 */
public class PersonMongoNameTest 
{
    private static final String PU = "mongoTest";


    private EntityManagerFactory emf;
    private EntityManager em;

    protected Map propertyMap = null;

    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);        
        emf = Persistence.createEntityManagerFactory(PU, propertyMap);
        em = emf.createEntityManager();

    }

    @Test
    public void test() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        Query findQuery = em.createQuery("Select p from PesonMongo p", PersonMongoNameAnnotation.class);
        List<PersonMongoNameAnnotation> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        findQuery = em.createQuery("Select p from PesonMongo p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        findQuery = em.createQuery("Select p.age from PesonMongo p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        PersonMongoNameAnnotation personWithKey = new PersonMongoNameAnnotation();
        personWithKey.setPersonId("111");
        em.persist(personWithKey);        

        em.clear();
        PersonMongoNameAnnotation p = findById(PersonMongoNameAnnotation.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Day.THURSDAY, p.getDay());

        em.clear();
        String qry = "Select p.personId,p.personName from PesonMongo p where p.personId >= 1";
        Query q = em.createQuery(qry);
        List<PersonMongoNameAnnotation> persons = q.getResultList();

        assertFindByName(em, "PersonEntityNameAnnotation", PersonMongoNameAnnotation.class, "vivek", "personName");  
        
        // Delete without WHERE clause.
        String deleteQuery = "DELETE from PesonMongo";
        q = em.createQuery(deleteQuery);
        q.executeUpdate();

    }
    
    @Test
    public void testEmptytableName() throws Exception
    {
        PersonMongoEmptyTableName p1 = new PersonMongoEmptyTableName();
        p1.setPersonId("1");
        p1.setPersonName("vivek");
        p1.setAge(10);
       

        Query findQuery = em.createQuery("Select p from PersonMongoEmptyTableName p", PersonMongoEmptyTableName.class);
        List<PersonMongoEmptyTableName> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        findQuery = em.createQuery("Select p from PersonMongoEmptyTableName p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        findQuery = em.createQuery("Select p.age from PersonMongoEmptyTableName p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);

        em.persist(p1);
     

        PersonMongoEmptyTableName personWithKey = new PersonMongoEmptyTableName();
        personWithKey.setPersonId("111");
        em.persist(personWithKey);        

        em.clear();
        PersonMongoEmptyTableName p = findById(PersonMongoEmptyTableName.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
       

        em.clear();
        String qry = "Select p.personId,p.personName from PersonMongoEmptyTableName p where p.personId = 1";
        Query q = em.createQuery(qry);
        List<PersonMongoEmptyTableName> persons = q.getResultList();

        
        
        // Delete without WHERE clause.
        String deleteQuery = "DELETE from PersonMongoEmptyTableName";
        q = em.createQuery(deleteQuery);
        q.executeUpdate();

    }
 
 
   

   

   

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        DummyDatabase.INSTANCE.dropDatabase();
    }
    

    private PersonMongoNameAnnotation prepareData(String rowKey, int age)
    {
        PersonMongoNameAnnotation o = new PersonMongoNameAnnotation();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.THURSDAY);
        return o;
    }

 
    private <E extends Object> E findById(Class<E> clazz, Object rowKey, EntityManager em)
    {
        return em.find(clazz, rowKey);
    }

 
    private <E extends Object> void assertFindByName(EntityManager em, String clazz, E e, String name,
            String fieldName)
    {

        String query = "Select p from PesonMongo p where p." + fieldName + " = " + name;
        // // find by name.
        Query q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
    }

   
   

 
    private <E extends Object> String getPersonName(E e, Object result)
    {

        return ((PersonMongoNameAnnotation) result).getPersonName();
    }
}

