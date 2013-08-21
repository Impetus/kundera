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
package com.impetus.client.es;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

/**
 * @author vivek.mishra
 * junit to demonstrate ESQuery implementation.
 */
public class PersonESTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
    }
    
    @Test
    public void testFindJPQL()
    {
        PersonES person = new PersonES();
        person.setAge(32);
        person.setDay(Day.FRIDAY);
        person.setPersonId("1");
        person.setPersonName("vivek");
        em.persist(person);
        
        
        person = new PersonES();
        person.setAge(32);
        person.setDay(Day.FRIDAY);
        person.setPersonId("2");
        person.setPersonName("kuldeep");
        em.persist(person);
        
        
        String queryWithOutAndClause = "Select p from PersonES p where p.personName = 'vivek'";
        Query nameQuery = em.createNamedQuery(queryWithOutAndClause);
        
        List<PersonES> persons = nameQuery.getResultList();
        
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("vivek", persons.get(0).getPersonName());
    
        String queryWithOutClause = "Select p from PersonES p";
        nameQuery = em.createNamedQuery(queryWithOutClause);
        
        persons = nameQuery.getResultList();
        
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());

        String invalidQueryWithAndClause = "Select p from PersonES p where p.personName = 'vivek' AND p.age = 34";
        nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        persons = nameQuery.getResultList();
        
        Assert.assertTrue(persons.isEmpty());


        String queryWithAndClause = "Select p from PersonES p where p.personName = 'vivek' AND p.age = 32";
        nameQuery = em.createNamedQuery(queryWithAndClause);
        persons = nameQuery.getResultList();
        
        Assert.assertFalse(persons.isEmpty());
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("vivek", persons.get(0).getPersonName());


        String queryWithORClause = "Select p from PersonES p where p.personName = 'vivek' OR p.personName = 'kuldeep'";
        nameQuery = em.createNamedQuery(queryWithORClause);
        persons = nameQuery.getResultList();
        
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());


        String invalidQueryWithORClause = "Select p from PersonES p where p.personName = 'vivek' OR p.personName = 'lkl'";
        nameQuery = em.createNamedQuery(invalidQueryWithORClause);
        persons = nameQuery.getResultList();
        
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
    }
}
