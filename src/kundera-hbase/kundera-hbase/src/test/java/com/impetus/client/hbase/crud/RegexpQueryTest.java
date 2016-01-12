/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.schemaManager.HBaseEntitySimple;

/**
 * The Class RegexpQueryTest.
 * 
 * @author karthikp.manchala
 */
public class RegexpQueryTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("hbase");
        em = emf.createEntityManager();
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
        emf.close();
    }

    /**
     * Regexp query test.
     */
    @Test
    public void regexpQueryTest()
    {
        init();
        em.clear();

        String qry = "Select p from HBaseEntitySimple p where p.personName regexp :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "^pra.*");
        List<HBaseEntitySimple> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("pragalbh garg", persons.get(0).getPersonName());

        qry = "Select p from HBaseEntitySimple p where p.personName regexp :name";
        q = em.createQuery(qry);
        q.setParameter("name", "^[pk]");
        persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(2, persons.size());

        qry = "Select p from HBaseEntitySimple p where p.personName regexp :name";
        q = em.createQuery(qry);
        q.setParameter("name", "pra...$");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        Assert.assertEquals("karthik prasad", persons.get(0).getPersonName());
    }

    /**
     * Inits the.
     */
    private void init()
    {
        HBaseEntitySimple p1 = new HBaseEntitySimple();
        p1.setAge((short) 23);
        p1.setPersonId("1");
        p1.setPersonName("pragalbh garg");

        HBaseEntitySimple p2 = new HBaseEntitySimple();
        p2.setAge((short) 20);
        p2.setPersonId("2");
        p2.setPersonName("karthik prasad");

        em.persist(p1);
        em.persist(p2);
    }

}