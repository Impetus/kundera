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
package com.impetus.kundera.client.cassandra.crud;


import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.composite.PrimeUser;
import com.impetus.kundera.client.cassandra.composite.UserTimeLine;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.query.Query;

/**
 * @author chhavi.gangwal
 * junit for {@link IResultIteratorEmbeddableTest}.
 */
public class ResultIteratorEmbeddableTest 
{
  

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;
    
   
    private static final String PERSISTENCE_UNIT = "ds_pu";
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        
        
        
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }


    
    @Test
    public void testScrollViaCQL3() throws Exception
    {
        onScroll();
    }

  
    private void onScroll()
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 15);
        Object p3 = prepareData("3", 20);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.flush();
        em.clear();
        final String queryWithoutClause = "Select p from PrimeUser p ";

        assertOnScroll(queryWithoutClause,3);

        final String queryWithClause = "Select p from PrimeUser p where p.name = vivek";
        
        assertOnScroll(queryWithClause,3);
        
        final String queryWithAndClause = "Select p from PrimeUser p where p.name = vivek and p.key.userId = 'mevivs2' and p.key.tweetId = 15";
        
        assertOnScroll(queryWithAndClause,1);

        final String queryWithLTClause = "Select p from PrimeUser p where p.name = vivek and p.key.userId = 'mevivs1' and p.key.tweetId < 15";
        
        assertOnScroll(queryWithLTClause,1);

        final String queryWithLTGTClause = "Select p from PrimeUser p where p.name = vivek and p.key.userId = 'mevivs2' and p.key.tweetId > 10 and p.key.tweetId < 20";
        
        assertOnScroll(queryWithLTGTClause,1);

              
        String queryWithIdClause = "Select p from PrimeUser p where p.key.userId = 'mevivs2' ";
        assertOnScroll(queryWithIdClause,1);
        
        String queryWithIdEmbeddableKeyClause = "Select p from PrimeUser p where p.key.userId = 'mevivs2' and p.key.tweetId = 15";
        assertOnScroll(queryWithIdEmbeddableKeyClause,1);
    }

    

    private void assertOnScroll(final String queryWithoutClause, int expectedCount)
    {
        Query query = (Query) em.createQuery(queryWithoutClause,
                PrimeUser.class);
        
        assertOnFetch(query, 3, expectedCount);
        assertOnFetch(query,2,expectedCount);  // less records

        assertOnFetch(query,4,expectedCount); // more fetch size than available in db.
        assertOnFetch(query,3,expectedCount); // more fetch size than available in db.
      
        assertOnFetch(query,null,expectedCount); //set to null; 
        
    }

    private void assertOnFetch(Query query, Integer fetchSize, int available)
    {
        query.setFetchSize(fetchSize);
        int counter=0;
        Iterator<PrimeUser> iter = query.iterate();

        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            counter++;
        }

        Assert.assertEquals(counter, fetchSize != null && fetchSize == 0 ? 0:available);
        try
        {
            iter.next();
            Assert.fail();
        }
        catch (NoSuchElementException nsex)
        {
            Assert.assertNotNull(nsex.getMessage());
        }
    }

    /**
     * CompositeUserDataType
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        CassandraCli.executeCqlQuery("TRUNCATE \"CompositeUser\"", "KunderaExamples");
    }
    
    private Object prepareData(String rowkey, int tweetId)
    {
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();   
        UserTimeLine key = new UserTimeLine("mevivs"+rowkey, tweetId, timeLineId);
        PrimeUser user = new PrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        user.setName("vivek");
        return user;
    }
    
   
}
