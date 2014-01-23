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
import java.util.HashSet;
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
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.polyglot.entities.AddressB1M;
import com.impetus.kundera.polyglot.entities.AddressBM1;
import com.impetus.kundera.polyglot.entities.PersonB1M;
import com.impetus.kundera.polyglot.entities.PersonBM1;
import com.impetus.kundera.query.Person.Day;
import com.impetus.kundera.utils.KunderaCoreUtils;
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
        p1.setSalary(6000.345);
        
        em.persist(p1);
        
        em.clear();
        
        Person p2 = new Person();
        p2.setAge(100);
        p2.setPersonId("2");
        p2.setDay(Day.SATURDAY);
        p2.setSalary(10000.345);
        
        em.persist(p2);
        
        String query = "Select p from Person p where p.personId = :personId";
        
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        
        KunderaQueryParser queryParser;
        KunderaQuery kunderaQuery = parseQuery(query);

        CoreQuery queryObj = new CoreQuery(kunderaQuery, delegator);
        
        queryObj.setParameter("personId", "1");
        List<Person> results = queryObj.getResultList();
        
        Assert.assertEquals(1,results.size());
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); //assert on lucene query transformation.
        Assert.assertNotNull(queryObj.populateUsingLucene()); //assert on lucene query transformation.
        
        final String deleteQuery = "Delete from Person p where p.personId = ?1";

        kunderaQuery = parseQuery(deleteQuery);
        
        
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
        try
        {
            Assert.assertNull(queryObj.getParameter("personId", String.class));
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertEquals("The parameter of the specified name does not exist or is not assignable to the type", iaex.getMessage());
        }
        Assert.assertNotNull(queryObj.getParameter(1, String.class));
        Assert.assertNotNull(queryObj.getParameterValue(1));
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene()); 

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
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
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
        
        queryObj.setFetchSize(100);

        Assert.assertEquals(new Integer(100),queryObj.getFetchSize());
        
        
        query = "Select p from Person p where p.personId = ?1";

        queryObj.setParameter(queryObj.getParameter("personId"),"1");
        
        results = queryObj.getResultList();
        
        Assert.assertEquals(0,results.size());
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene()); 
        

        //TODO:: commented out a single methoda have been introduced in QueryImpl, used from test package only!
       /* Set luceneResults = queryObj.fetchByLuceneQuery();
        Assert.assertNotNull(luceneResults);            // assert of lucene index search result.
        Assert.assertEquals(1,luceneResults.size());
        */
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
            kunderaQuery = parseQuery(query);
            queryObj = new CoreQuery(kunderaQuery, delegator);
            Assert.assertNull(queryObj.getParameter(1, String.class));
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalStateException iaex)
        {
            Assert.assertEquals("invoked on a native query when the implementation does not support this use", iaex.getMessage());
        }

        
        try
        {
            final String updateQuery = "Update Person p set p.personName=Amresh where p.personId = 1";
            kunderaQuery = parseQuery(updateQuery);       
            queryObj = new CoreQuery(kunderaQuery, delegator);
            queryObj.executeUpdate();
            Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
            Assert.assertNotNull(queryObj.populateUsingLucene()); 
        }
        catch (Exception e)
        {
            
            Assert.fail(e.getMessage());
        }        
        
        try
        {
            queryObj.unwrap(Client.class);
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
        
        
        query = "Select p from Person p where p.age >:age";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("age", new Integer(32));       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene());
        
        query = "Select p from Person p where p.age >=:age";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("age", new Integer(32));       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene());
        
        query = "Select p from Person p where p.age <:age";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("age", new Integer(32));       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene());
        
        query = "Select p from Person p where p.age <=:age";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("age", new Integer(32));       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene());
        
        query = "Select p from Person p where p.personName like :personName";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("personName", "Amresh");       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene()); 
        
        query = "Select p from Person p where p.salary >=:salary";        
        delegator = CoreTestUtilities.getDelegator(em);  
        kunderaQuery = parseQuery(query);
        queryObj = new CoreQuery(kunderaQuery, delegator);        
        queryObj.setParameter("salary", 500.0);       
        Assert.assertNotNull(KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery)); 
        Assert.assertNotNull(queryObj.populateUsingLucene());
        
        onassertBi1MAssociation(delegator);
        onassertBiM1Association(delegator);

}

    private void onassertBi1MAssociation(PersistenceDelegator delegator)
    {
        KunderaQuery kunderaQuery;
        CoreQuery queryObj;
        PersonB1M personBi1M = new PersonB1M();
        
        personBi1M.setPersonId("personBi1M1");
        personBi1M.setPersonName("impetus-opensource");
        
        AddressB1M addressBi1M = new AddressB1M();
        addressBi1M.setAddressId("addrBi1M1");
        addressBi1M.setStreet("meri gali");
        Set<AddressB1M> addresses = new HashSet<AddressB1M>();
        addresses.add(addressBi1M);
//        personBi1M.setAddresses(addresses);
        
        AddressB1M addressBi1M_copy = new AddressB1M();
        addressBi1M_copy.setAddressId("addrBi1M2");
        addressBi1M_copy.setStreet("meri gali");
        addresses.add(addressBi1M_copy);
        personBi1M.setAddresses(addresses);

        em.persist(personBi1M);
        
        em.clear();
        
        String query = "Select p from PersonB1M p where p.personId = 'personBi1M1'";

        kunderaQuery = parseQuery(query);
        
        
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
        List<PersonB1M> associationResults = queryObj.getResultList();
        
        Assert.assertTrue(!associationResults.isEmpty());
        
        Assert.assertNotNull(associationResults.get(0).getAddresses());
        
        Assert.assertEquals(2,associationResults.get(0).getAddresses().size());
        
        
        query = "Select p from PersonB1M p where p.personId = 'invalid'";

        kunderaQuery = parseQuery(query);
        
        
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
        associationResults = queryObj.getResultList();

        Assert.assertTrue(associationResults.isEmpty());

    }

    private void onassertBiM1Association(PersistenceDelegator delegator)
    {
        KunderaQuery kunderaQuery;
        CoreQuery queryObj;
        
        PersonBM1 personBiM11 = new PersonBM1();
        personBiM11.setPersonId("personBiM11");
        personBiM11.setPersonName("impetus-opensource");
        

        PersonBM1 personBiM12 = new PersonBM1();
        personBiM12.setPersonId("personBiM12");
        personBiM12.setPersonName("impetus-opensource");

        AddressBM1 addressBiM1 = new AddressBM1();
        addressBiM1.setAddressId("addrBiM11");
        addressBiM1.setStreet("meri gali");

        
        personBiM11.setAddress(addressBiM1);
        personBiM12.setAddress(addressBiM1);

        em.persist(personBiM11);
        em.persist(personBiM12);
        
        em.clear();
        
        String selectAssociationQuery = "Select p from PersonBM1 p";

        kunderaQuery = parseQuery(selectAssociationQuery);
        
        
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
        List<PersonBM1> associationResults = queryObj.getResultList();
        
        Assert.assertTrue(!associationResults.isEmpty());
        
        Assert.assertNotNull(associationResults.get(0).getAddress());
        
        
        selectAssociationQuery = "Select p from PersonBM1 p where p.personId = 'invalid'";

        kunderaQuery = parseQuery(selectAssociationQuery);        
        
        queryObj = new CoreQuery(kunderaQuery, delegator);
        
        associationResults = queryObj.getResultList();

        Assert.assertTrue(associationResults.isEmpty());

    }

    private KunderaQuery parseQuery(final String query)
    {
        KunderaQuery kunderaQuery = new KunderaQuery(query);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
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

        CoreQuery query = new CoreQuery(kunderaQuery, delegator);

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
