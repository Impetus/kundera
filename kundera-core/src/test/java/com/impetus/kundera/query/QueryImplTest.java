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

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.TemporalType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.Person.Day;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

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
        
        String query = "Select p from Person p where p.personId = :personId";
        
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        
        KunderaQueryParser queryParser;
        KunderaQuery kunderaQuery = parseQuery(query);

        CoreQuery queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        queryObj.setParameter("personId", "1");
        List<Person> results = queryObj.getResultList();
        
        Assert.assertEquals(1,results.size());

        
        final String deleteQuery = "Delete from Person p where p.personId = ?1";

        kunderaQuery = parseQuery(deleteQuery);
        
        
        queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        try
        {
            Assert.assertNull(queryObj.getParameter("personId", String.class));
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertEquals("The parameter of the specified name does not exist or is not assignable to the type", iaex.getMessage());
        }
        Assert.assertNotNull(queryObj.getParameter(1, String.class));
        Assert.assertNotNull(queryObj.getParameterValue(1));

        try
        {
            queryObj.getParameterValue(1);
        } catch(IllegalStateException usex)
        {
            Assert.assertEquals("parameter has not been bound" + 1, usex.getMessage());
        }

        queryObj.setParameter(1, "1");
        
        queryObj.executeUpdate();
        
        
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(query, kunderaQuery, delegator);
        
        try
        {
            queryObj.setParameter(CoreTestUtilities.getParameter(), "test");
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
        
        try
        {
            queryObj.setParameter(CoreTestUtilities.getParameter("personId","1"), "1");
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
        
        queryObj.setParameter(queryObj.getParameter("personId"),"1");
        
        results = queryObj.getResultList();
        
        Assert.assertEquals(0,results.size());
        
        queryObj.setHint("test", "test");
        
        queryObj.setMaxResults(100);
        Assert.assertEquals(100, queryObj.getMaxResults());

        Assert.assertNotNull(queryObj.getHints());
        
        
        query = "Select p from Person p where p.personId = ?1";

        queryObj.setParameter(queryObj.getParameter("personId"),"1");
        
        results = queryObj.getResultList();
        
        Assert.assertEquals(0,results.size());
        
        Assert.assertNotNull(queryObj.getLuceneQueryFromJPAQuery()); //assert on lucene query transformation.
        
        Set luceneResults = queryObj.fetchByLuceneQuery();
        Assert.assertNotNull(luceneResults);            // assert of lucene index search result.
        Assert.assertEquals(1,luceneResults.size());
        
        Assert.assertNotNull(queryObj.getParameter("personId", String.class));
        
        Assert.assertTrue(queryObj.isBound(queryObj.getParameter("personId", String.class)));
        
        Assert.assertNotNull(queryObj.getParameterValue(queryObj.getParameter("personId", String.class)));
        Assert.assertNotNull(queryObj.getParameterValue("personId"));
        
        try
        {
            Assert.assertNull(queryObj.getParameter(1, String.class));
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
       
        try
        {
            queryObj.getParameter(1);
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
        //assert on native query.
        try
        {
            ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
            appMetadata.addQueryToCollection(query, query, true, null);
            queryObj = new CoreQuery(query, kunderaQuery, delegator);
            Assert.assertNull(queryObj.getParameter(1, String.class));
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalStateException iaex)
        {
            Assert.assertEquals("invoked on a native query when the implementation does not support this use", iaex.getMessage());
        }
        
        
        try
        {
            queryObj.unwrap(Integer.class);
        } catch(ClassCastException usex)
        {
            Assert.assertEquals("Provider does not support the call for class type:[" + Integer.class + "]", usex.getMessage());
        }
        
        try
        {
            queryObj.getParameterValue("invalidParameter");
        } catch(IllegalArgumentException usex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", usex.getMessage());
        }
        
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
        LuceneCleanupUtilities.cleanLuceneDirectory(PU);
    }

    /**
     * @param query
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     * @throws SecurityException 
     * @throws NoSuchFieldException 
     */
    @Test
    public void assertOnUnsupportedMethod() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        String queryStr = "Select p from Person p where p.personId = :personId";
        
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        
        KunderaQueryParser queryParser;
        KunderaQuery kunderaQuery = parseQuery(queryStr);

        CoreQuery query = new CoreQuery(queryStr, kunderaQuery, delegator);

        try
        {
            query.setFlushMode(FlushModeType.AUTO);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFlushMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setFirstResult(1);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFirstResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getSingleResult();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getSingleResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getFirstResult();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFirstResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setLockMode(LockModeType.NONE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setLockMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getLockMode();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getLockMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter(0,new Date(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter("param",new Date(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter(0,Calendar.getInstance(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter("param",Calendar.getInstance(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(CoreTestUtilities.getParameter(),Calendar.getInstance(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(CoreTestUtilities.getParameter(),new Date(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.getFlushMode();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFlushMode is unsupported by Kundera", usex.getMessage());
        }

       

    }

}
