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
package com.impetus.client.query;

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

/**
 * The Class LikeQueryTest.
 */
public class LikeQueryTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("queryTest");
        em = emf.createEntityManager();
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    /**
     * Like query test.
     */
    @Test
    public void likeQueryTest()
    {
        init();
        em.clear();

        String qry = "Select p from HBaseEntitySimple p where p.personName like :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "pragal");
        List<HBaseEntitySimple> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(1, persons.size());

        qry = "Select p from HBaseEntitySimple p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "thik");
        persons = q.getResultList();
        assertEquals(1, persons.size());
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
        p2.setAge((short)20);
        p2.setPersonId("2");
        p2.setPersonName("karthik prasad");

        em.persist(p1);
        em.persist(p2);
    }

}