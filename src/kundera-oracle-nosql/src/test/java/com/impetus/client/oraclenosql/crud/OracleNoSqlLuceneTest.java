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
package com.impetus.client.oraclenosql.crud;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.PersonOracleNoSql;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * 
 * @author shaheed.hussain
 * 
 */
public class OracleNoSqlLuceneTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setUp()
    {
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put("index.home.dir", "./lucene");
        emf = Persistence.createEntityManagerFactory("twikvstore", propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void luceneTest()
    {
        persistPerson("1", "person1", 10);
        persistPerson("2", "person2", 20);
        persistPerson("3", "person3", 30);
        persistPerson("4", "person4", 40);

        clearEm();
        // Select query, without where clause
        String findWithOutWhereClause = "Select p from PersonOracleNoSql p";
        List<PersonOracleNoSql> results = executeSelectQuery(findWithOutWhereClause);
        Assert.assertEquals(4, results.size());

        clearEm();
        String findSelective = "Select p.age from PersonOracleNoSql p";
        results = executeSelectQuery(findSelective);
        Assert.assertEquals(4, results.size());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

        clearEm();
        // Select query with where clause on single non-ID column
        String findByName = "Select p.personId from PersonOracleNoSql p where p.personName='person1'";
        results = executeSelectQuery(findByName);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("1", results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        clearEm();
        // Select query with where clause on ID column
        String findById = "Select p from PersonOracleNoSql p where p.personId=2";
        results = executeSelectQuery(findById);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("2", results.get(0).getPersonId());
        Assert.assertEquals("person2", results.get(0).getPersonName());
        Assert.assertEquals(20, results.get(0).getAge().intValue());

        String find = "Select p from PersonOracleNoSql p where p.personName='person4' and p.age>30";
        results = executeSelectQuery(find);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("4", results.get(0).getPersonId());
        Assert.assertEquals("person4", results.get(0).getPersonName());
        Assert.assertEquals(40, results.get(0).getAge().intValue());

        // Delete by query.
        String deleteQuery = "Delete from PersonOracleNoSql p";
        Query query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();
        Assert.assertEquals(4, updateCount);
    }

    private void persistPerson(String personId, String personName, int age)
    {
        PersonOracleNoSql p = preparePerson(personId, age, personName);
        em.persist(p);
    }

    private PersonOracleNoSql preparePerson(String rowKey, int age, String name)
    {
        PersonOracleNoSql person = new PersonOracleNoSql();
        person.setPersonId(rowKey);
        person.setPersonName(name);
        person.setAge(age);
        return person;
    }

    protected void clearEm()
    {
        em.clear();
    }

    protected List<PersonOracleNoSql> executeSelectQuery(String jpaQuery)
    {
        Query query = em.createQuery(jpaQuery);
        return query.getResultList();
    }

    @After
    public void tearDown() throws Exception
    {
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("twikvstore"));
        em.close();
        emf.close();
    }
    
}
