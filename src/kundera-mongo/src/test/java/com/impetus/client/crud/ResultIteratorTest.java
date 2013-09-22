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
package com.impetus.client.crud;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.Query;

/**
 * @author kuldeep.mishra junit for {@link IResultIterator}.
 */
public class ResultIteratorTest extends BaseTest
{
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
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
    }

    @Test
    public void testScroll() throws Exception
    {
        onScroll();
    }

    @Test
    public void testScrollAssociation() throws Exception
    {
        assertOnTokenScroll();
    }

    private void assertOnTokenScroll()
    {
        MongoToken token1 = new MongoToken();
        token1.setId("MongoTokenId1");
        token1.setTokenName("tokenName1");
        MongoTokenClient client = new MongoTokenClient();
        client.setClientName("tokenClient1");
        client.setId("tokenClientId");
        token1.setClient(client);

        MongoToken token2 = new MongoToken();
        token2.setId("tokenId2");
        token2.setTokenName("tokenName2");
        token2.setClient(client);
        em.persist(token1);
        em.persist(token2);

        String queryWithoutClause = "Select t from MongoToken t";
        assertOnTokenScroll(queryWithoutClause, 2);

        String queryWithClause = "Select t from MongoToken t where t.tokenName='tokenName1'";

        assertOnTokenScroll(queryWithClause, 1);

        // TODO:: Need to discuss with KK, this should be working with token
        // support. Special scenario.
        String queryWithIdClause = "Select t from MongoToken t where t.tokenId = 'MongoTokenId1'";
        //
        assertOnTokenScroll(queryWithIdClause, 1);

    }

    private void assertOnTokenScroll(String queryClause, int expected)
    {
        Query query = (Query) em.createQuery(queryClause, MongoToken.class);

        int count = 0;
        Iterator<MongoToken> tokens = query.iterate();
        while (tokens.hasNext())
        {
            MongoToken token = tokens.next();
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
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.flush();
        em.clear();
        final String queryWithoutClause = "Select p from PersonMongo p";

        assertOnScroll(queryWithoutClause, 3);

        final String queryWithClause = "Select p from PersonMongo p where p.personName = vivek";

        assertOnScroll(queryWithClause, 3);

        final String queryWithAndClause = "Select p from PersonMongo p where p.personName = vivek and p.age = 15";

        assertOnScroll(queryWithAndClause, 1);

        final String queryWithLTClause = "Select p from PersonMongo p where p.personName = vivek and p.age < 15";

        assertOnScroll(queryWithLTClause, 1);

        final String queryWithGTClause = "Select p from PersonMongo p where p.personName = vivek and p.age >= 15";

        assertOnScroll(queryWithGTClause, 2);

        final String queryWithLTGTClause = "Select p from PersonMongo p where p.personName = vivek and p.age > 10 and p.age < 20";

        assertOnScroll(queryWithLTGTClause, 1);

        final String queryWithLTGTEClause = "Select p from PersonMongo p where p.personName = vivek and p.age >= 10 and p.age < 20";

        assertOnScroll(queryWithLTGTEClause, 2);

        String queryWithIdClause = "Select p from PersonMongo p where p.personId = '2' ";
        assertOnScroll(queryWithIdClause, 1);
    }

    private void assertOnScroll(final String queryWithoutClause, int expectedCount)
    {
        Query query = (Query) em.createQuery(queryWithoutClause, PersonMongo.class);

        assertOnFetch(query, 0, expectedCount);
        assertOnFetch(query, 2, expectedCount); // less records

        assertOnFetch(query, 4, expectedCount); // more fetch size than
                                                // available in db.
        assertOnFetch(query, 3, expectedCount); // more fetch size than
                                                // available in db.

        assertOnFetch(query, null, expectedCount); // set to null;

    }

    private void assertOnFetch(Query query, Integer fetchSize, int available)
    {
        query.setFetchSize(fetchSize);
        int counter = 0;
        Iterator<PersonMongo> iter = query.iterate();

        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            counter++;
        }

        Assert.assertEquals(counter, fetchSize == null || available < fetchSize ? available : fetchSize);
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

    @After
    public void tearDown()
    {

    }
}