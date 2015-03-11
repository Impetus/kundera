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
package com.impetus.client.hbase.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

import com.impetus.client.hbase.crud.PersonHBase.Day;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author Shaheed.Hussain
 * 
 */
public class HBaseLuceneTest extends BaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    protected Map<String, String> propertyMap = new HashMap<String, String>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap.put("index.home.dir", "lucene");
        emf = Persistence.createEntityManagerFactory("hbaseTest", propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("hbaseTest"));
        LuceneCleanupUtilities.cleanDir("./lucene");
        emf.close();
    }

    @Test
    public void likeQueryTest()
    {
        init();
        em.clear();

        String qry = "Select p from PersonHBase p where p.personName like :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "vi");
        List<PersonHBase> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(3, persons.size());

        qry = "Select p from PersonHBase p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "pragalbh");
        persons = q.getResultList();
        assertEquals(0, persons.size());
        
        PersonHBase p = new PersonHBase();
        p.setAge(20);
        p.setDay(Day.MONDAY);
        p.setMonth(Month.JAN);
        p.setPersonId("4");
        p.setPersonName("pragalbh garg");
        
        PersonHBase g = new PersonHBase();
        g.setAge(20);
        g.setDay(Day.MONDAY);
        g.setMonth(Month.JAN);
        g.setPersonId("5");
        g.setPersonName("karthik prasad");
        
        em.persist(p);
        em.persist(g);
        
        qry = "Select p from PersonHBase p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "garg");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from PersonHBase p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "karthik ");
        persons = q.getResultList();
        assertEquals(1, persons.size());
    }

    @Test
    public void test()
    {
        init();
        em.clear();
        
        String qry = "Select p from PersonHBase p where p.personName = :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "vivek");
        List<PersonHBase> persons = q.getResultList();
        Assert.assertEquals(3, persons.size());

        qry = "Select p.personName, p.age from PersonHBase p where p.personId = 1 and p.age = 10";
        q = em.createQuery(qry);
        persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertEquals(10, persons.get(0).getAge().intValue());
        assertEquals("vivek", persons.get(0).getPersonName());

        qry = "Select p.personName from PersonHBase p where p.age=20";
        q = em.createQuery(qry);
        persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getAge());
        assertEquals("vivek", persons.get(0).getPersonName());

        qry = "Select p.age from PersonHBase p where p.personId = 2";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getPersonName());
        assertEquals(20, persons.get(0).getAge().intValue());

        qry = "Select p from PersonHBase p";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        
        PersonHBase p = new PersonHBase();
        p.setAge(20);
        p.setDay(Day.MONDAY);
        p.setMonth(Month.JAN);
        p.setPersonId("4");
        p.setPersonName("pragalbh garg");
        em.persist(p);
        
        qry = "Select p from PersonHBase p where p.personName = :name";
        q = em.createQuery(qry);
        q.setParameter("name", "pragalbh garg");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        
        qry = "Select p from PersonHBase p where p.personName = :name";
        q = em.createQuery(qry);
        q.setParameter("name", "pragalbh g");
        persons = q.getResultList();
        assertEquals(0, persons.size());

    }

    private void init()
    {
        Object p1 = prepareHbaseInstance("1", 10);
        Object p2 = prepareHbaseInstance("2", 20);
        Object p3 = prepareHbaseInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
    }
}