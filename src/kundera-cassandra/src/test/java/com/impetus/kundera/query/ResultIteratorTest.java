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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Assert;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.BaseTest;
import com.impetus.client.crud.PersonCassandra;
import com.impetus.client.crud.Token;
import com.impetus.client.crud.TokenClient;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author vivek.mishra
 * junit for {@link IResultIterator}.
 */
public class ResultIteratorTest extends BaseTest
{
    private static final String SEC_IDX_CASSANDRA_TEST = "secIdxCassandraTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;


    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    public void setUp(final String persistenceUnit,final String keyspace, final String cqlVersion) throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(keyspace);
        Map propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        propertyMap.put(CassandraConstants.CQL_VERSION, cqlVersion);

        emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void testScrollViaCQL3() throws Exception
    {
        setUp(SEC_IDX_CASSANDRA_TEST,"KunderaExamples",CassandraConstants.CQL_VERSION_3_0);
        onScroll();
        tearDown("KunderaExamples");
    }

    @Test
    public void testScrollViaCQL2() throws Exception
    {
        setUp(SEC_IDX_CASSANDRA_TEST,"KunderaExamples",CassandraConstants.CQL_VERSION_3_0);
        onScroll();
        tearDown("KunderaExamples");
    }

    @Test
    public void testCQL3ScrollAssociation() throws Exception 
    {
        setUp("myapp_pu", "myapp", CassandraConstants.CQL_VERSION_3_0);
        assertOnTokenScroll();
        tearDown("myapp");
    }
    
    @Test
    public void testCQL2ScrollAssociation() throws Exception 
    {
        setUp("myapp_pu", "myapp", CassandraConstants.CQL_VERSION_2_0);
        assertOnTokenScroll();
        tearDown("myapp");
    }

    private void assertOnTokenScroll()
    {
        Token token1 = new Token();
        token1.setId("tokenId1");
        token1.setTokenName("tokenName1");
        TokenClient client = new TokenClient();
        client.setClientName("tokenClient1");
        client.setId("tokenClientId");
        token1.setClient(client);

        Token token2 = new Token();
        token2.setId("tokenId2");
        token2.setTokenName("tokenName2");
        token2.setClient(client);
        em.persist(token1);
        em.persist(token2);

        String queryWithoutClause = "Select t from Token t";
        assertOnTokenScroll(queryWithoutClause,2);

    
        String queryWithClause = "Select t from Token t where t.tokenName='tokenName1'";
        
        assertOnTokenScroll(queryWithClause,1);

        //TODO:: Need to discuss with KK, this should be working with token support. Special scenario. 
        String queryWithIdClause = "Select t from Token t where t.id = 'tokenId1'";
//        
        assertOnTokenScroll(queryWithIdClause,1);
        

    }

    private void assertOnTokenScroll(String queryClause, int expected)
    {
        Query<Token> query = (Query<Token>) em.createQuery(queryClause,
                Token.class);
        
        int count=0;
        Iterator<Token> tokens = query.iterate();
        while(tokens.hasNext())
        {
            Token token = tokens.next();
            Assert.assertNotNull(token);
            Assert.assertNotNull(token.getClient());
            Assert.assertEquals(2, token.getClient().getTokens().size());
            count++;
        }
        
        Assert.assertTrue(count > 0);
        Assert.assertTrue(count == expected);
    }
    
    private void onScroll()
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.flush();
        em.clear();
        final String queryWithoutClause = "Select p from PersonCassandra p";

        assertOnScroll(queryWithoutClause,3);

        final String queryWithClause = "Select p from PersonCassandra p where p.personName = vivek";
        
        assertOnScroll(queryWithClause,3);
        
        final String queryWithAndClause = "Select p from PersonCassandra p where p.personName = vivek and p.age = 15";
        
        assertOnScroll(queryWithAndClause,1);

        final String queryWithLTClause = "Select p from PersonCassandra p where p.personName = vivek and p.age < 15";
        
        assertOnScroll(queryWithLTClause,1);

        final String queryWithGTClause = "Select p from PersonCassandra p where p.personName = vivek and p.age >= 15";
        
        assertOnScroll(queryWithGTClause,2);

        final String queryWithLTGTClause = "Select p from PersonCassandra p where p.personName = vivek and p.age > 10 and p.age < 20";
        
        assertOnScroll(queryWithLTGTClause,1);

        final String queryWithLTGTEClause = "Select p from PersonCassandra p where p.personName = vivek and p.age >= 10 and p.age < 20";
        
        assertOnScroll(queryWithLTGTEClause,2);
        
        String queryWithIdClause = "Select p from PersonCassandra p where p.personId = '2' ";
        assertOnScroll(queryWithIdClause,1);
    }

    private void assertOnScroll(final String queryWithoutClause, int expectedCount)
    {
        Query<PersonCassandra> query = (Query<PersonCassandra>) em.createQuery(queryWithoutClause,
                PersonCassandra.class);
        
        assertOnFetch(query, 0, expectedCount);
        assertOnFetch(query,2,expectedCount);  // less records

        assertOnFetch(query,4,expectedCount); // more fetch size than available in db.
        assertOnFetch(query,3,expectedCount); // more fetch size than available in db.
        
        assertOnFetch(query,null,expectedCount); //set to null; 
        
    }

    private void assertOnFetch(Query<PersonCassandra> query, Integer fetchSize, int available)
    {
        query.setFetchSize(fetchSize);
        int counter=0;
        Iterator<PersonCassandra> iter = query.iterate();

        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            counter++;
        }

        Assert.assertEquals(counter, fetchSize == null || available < fetchSize?available:fetchSize);
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

    public void tearDown(final String keyspace)
    {
        CassandraCli.dropKeySpace(keyspace);
    }
}
