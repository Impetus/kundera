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
     * @throws Exception
     *             the exception
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
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        removePersons();
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

        String qry = "Select p from HBasePerson p where p.personName like :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "pragal");
        List<HBasePerson> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("pragalbh garg", persons.get(0).getPersonName());

        qry = "Select p from HBasePerson p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "thik");
        persons = q.getResultList();
        assertEquals(1, persons.size());
        assertEquals("karthik prasad", persons.get(0).getPersonName());
        
        qry = "Select p from HBasePerson p where p.personId like :id";
        q = em.createQuery(qry);
        q.setParameter("id", "abc");
        persons = q.getResultList();
        assertEquals(2, persons.size());
    }

    /**
     * Inits the.
     */
    private void init()
    {
        HBasePerson p1 = new HBasePerson();
        p1.setAge((short) 23);
        p1.setPersonId("12_abc_56");
        p1.setPersonName("pragalbh garg");

        HBasePerson p2 = new HBasePerson();
        p2.setAge((short) 20);
        p2.setPersonId("45_abc_34");
        p2.setPersonName("karthik prasad");

        em.persist(p1);
        em.persist(p2);
    }

    /**
     * Remove Persons.
     */
    private void removePersons()
    {
        HBasePerson p1 = em.find(HBasePerson.class, "12_abc_56");
        em.remove(p1);
        HBasePerson p2 = em.find(HBasePerson.class, "45_abc_34");
        em.remove(p2);
    }

}