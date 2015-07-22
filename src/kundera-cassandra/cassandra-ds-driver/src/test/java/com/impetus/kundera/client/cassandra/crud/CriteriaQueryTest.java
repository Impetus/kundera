/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.kundera.client.cassandra.crud;

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

import com.impetus.client.crud.BaseTest;
import com.impetus.client.crud.Month;
import com.impetus.client.crud.PersonCassandra;
import com.impetus.client.crud.PersonCassandra.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Test case to perform simple CRUD operation.(select using CriteriaBuilder 
 * @author vivek.mishra
 */
public class CriteriaQueryTest extends BaseTest
{
    private static final String _PU = "cassandra_ds_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager entityManager;

    /** The col. */
    private Map<Object, Object> col;

    protected Map propertyMap = null;


    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        
        CassandraCli.cassandraSetUp();
        System.setProperty("cassandra.start_native_transport", "true");

        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }

        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        entityManager = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * On insert cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertCassandra() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        Query findQuery = entityManager.createQuery("Select p from PersonCassandra p", PersonCassandra.class);
        List<PersonCassandra> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = entityManager.createQuery("Select p.age from PersonCassandra p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        PersonCassandra personWithKey = new PersonCassandra();
        personWithKey.setPersonId("111");
        entityManager.persist(personWithKey);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);

        entityManager.clear();
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Day.thursday, p.getDay());

        entityManager.clear();
        Query q;
        List<PersonCassandra> persons = queryOverRowkey();

        assertFindByName(entityManager, PersonCassandra.class, PersonCassandra.class, "vivek", "personName");
        assertFindByNameAndAge(entityManager, PersonCassandra.class, PersonCassandra.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(entityManager, PersonCassandra.class, PersonCassandra.class, "vivek", "10", "20",
                "personName");
        assertFindByNameAndAgeBetween(entityManager, PersonCassandra.class, PersonCassandra.class, "vivek", "10", "15",
                "personName");
        assertFindByRange(entityManager, PersonCassandra.class, PersonCassandra.class, "1", "2", "personId", true);
        assertFindWithoutWhereClause(entityManager, PersonCassandra.class, PersonCassandra.class, true);

        // perform merge after query.
        for (PersonCassandra person : persons)
        {
            person.setPersonName("'after merge'");
            entityManager.merge(person);
        }

        entityManager.clear();

        p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("'after merge'", p.getPersonName());

        String updateQuery = "update PersonCassandra p set p.personName='KK MISHRA' where p.personId=1";
        q = entityManager.createQuery(updateQuery);
        q.executeUpdate();

        entityManager.clear();
        p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("KK MISHRA", p.getPersonName());

        updateQuery = "update PersonCassandra p set p.personName='Screenshot from 2013-10-30 17:43:51.png' where p.personId=1";
        q = entityManager.createQuery(updateQuery);
        q.executeUpdate();

        entityManager.clear();
        assertFindByNameWithReservedKeywords(entityManager, PersonCassandra.class, PersonCassandra.class, "Screenshot from 2013-10-30 17:43:51.png", "personName");
        
        entityManager.clear();
        updateQuery = "update PersonCassandra p set p.personName='john.dever@impetus.co.in' where p.personId=1";
        q = entityManager.createQuery(updateQuery);
        q.executeUpdate();

        entityManager.clear();
        assertFindByNameWithReservedKeywords(entityManager, PersonCassandra.class, PersonCassandra.class, "john.dever@impetus.co.in", "personName");
        
        
        // Delete without WHERE clause.

        String deleteQuery = "DELETE from PersonCassandra";
        q = entityManager.createQuery(deleteQuery);
        Assert.assertEquals(4, q.executeUpdate());

    }

    private List<PersonCassandra> queryOverRowkey()
    {
        String qry = "Select p.personId,p.personName from PersonCassandra p where p.personId = 1";
        Query q = entityManager.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId > 1";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId < 2";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId <= 2";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId >= 1";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        return persons;
    }


    /**
     * On merge cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onMergeCassandra() throws Exception
    {
        // CassandraCli.cassandraSetUp();
        // loadData();
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        entityManager.clear();
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Month.APRIL, p.getMonth());
        // modify record.
        p.setPersonName("newvivek");
        entityManager.merge(p);

        assertOnMerge(entityManager, PersonCassandra.class, PersonCassandra.class, "vivek", "newvivek", "personName");
    }



    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        entityManager.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    private class HandlePersist implements Runnable
    {
        private int i;

        public HandlePersist(int i)
        {
            this.i = i;
        }

        @Override
        public void run()
        {
            for (int j = i * 1000; j < (i + 1) * 1000; j++)
            {
                entityManager.persist(prepareData("" + j, j + 10));
            }
        }
    }

}
