/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.query;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.Person.Day;

/**
 * @author vivek.mishra
 * junit for {@link QueryImpl}
 *
 */
public class QueryImplTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);        
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Person p1 = new Person();
        p1.setAge(98);
        p1.setPersonId("1");
        p1.setDay(Day.SATURDAY);
        
        em.persist(p1);
        
        em.clear();
        
        Person p2 = new Person();
        p2.setAge(100);
        p2.setPersonId("2");
        p2.setDay(Day.SATURDAY);
        
        em.persist(p2);
        
        final String query = "Select p from Person p where p.personId = '1'";
        
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        
        KunderaQueryParser queryParser;
        KunderaQuery kunderaQuery = parseQuery(query);

        CoreQuery queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        List<Person> results = queryObj.getResultList();
        
        Assert.assertEquals(1,results.size());

        
        final String deleteQuery = "Delete from Person p where p.personId = '1'";

        kunderaQuery = parseQuery(deleteQuery);
        queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        queryObj.executeUpdate();
        
        
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        results = queryObj.getResultList();
        
        Assert.assertEquals(0,results.size());
    }

    private KunderaQuery parseQuery(final String query)
    {
        KunderaQuery kunderaQuery = new KunderaQuery();
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery, query);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        return kunderaQuery;
    }


    @After
    public void tearDown()
    {
        DummyDatabase.INSTANCE.dropDatabase();
    }
}
