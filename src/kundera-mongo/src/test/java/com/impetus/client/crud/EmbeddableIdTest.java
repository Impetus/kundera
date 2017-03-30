/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.client.crud;

import com.impetus.client.crud.entities.CompositeId;
import com.impetus.client.crud.entities.CompositeUser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.List;

/**
 * This test verifies embedded IDs work in queries when the field name is
 * different from the column name.
 */
public class EmbeddableIdTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up.
     */
    @Before
    public void setUp()
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();

        prepareData();
    }

    /**
     * Test select.
     */
    @Test
    public void testSelect()
    {
        Query query = null;
        List<?> results = null;

        query = em.createQuery("select u from CompositeUser u");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 3);

        query = em.createQuery("select u from CompositeUser u where u.id.birthDate = :year");
        query.setParameter("year", "1986");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 1);

        query = em.createQuery("select u from CompositeUser u where u.id.birthDate <= :year");
        query.setParameter("year", "1986");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 2);
    }

    /**
     * Prepare data.
     */
    private void prepareData()
    {
        CompositeId id1 = new CompositeId();
        id1.setFirstName("John");
        id1.setBirthDate("1981");

        CompositeUser user1 = new CompositeUser();
        user1.setId(id1);
        user1.setPhone("90001");

        em.persist(user1);

        CompositeId id2 = new CompositeId();
        id2.setFirstName("Carl");
        id2.setBirthDate("1988");

        CompositeUser user2 = new CompositeUser();
        user2.setId(id2);
        user2.setPhone("90002");

        em.persist(user2);

        CompositeId id3 = new CompositeId();
        id3.setFirstName("Viktor");
        id3.setBirthDate("1986");

        CompositeUser user3 = new CompositeUser();
        user3.setId(id3);
        user3.setPhone("90003");

        em.persist(user3);
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        em.close();
        emf.close();
    }

}
