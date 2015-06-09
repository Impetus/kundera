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
package com.impetus.client.couchdb.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.couchdb.datatypes.tests.CouchDBBase;
import com.impetus.client.couchdb.entities.CouchDBToken;
import com.impetus.client.couchdb.entities.CouchDBTokenClient;
import com.impetus.client.couchdb.entities.Month;
import com.impetus.client.couchdb.entities.PersonCouchDB;
import com.impetus.client.couchdb.entities.PersonCouchDB.Day;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.Query;

/**
 * @author Kuldeep.mishra junit for {@link IResultIterator}.
 */
public class ResultIteratorTest extends CouchDBBase
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

        emf = Persistence.createEntityManagerFactory("couchdb_pu");
        super.setUpBase(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
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
        CouchDBToken token1 = new CouchDBToken();
        token1.setId("tokenId1");
        token1.setTokenName("tokenName1");
        CouchDBTokenClient client = new CouchDBTokenClient();
        client.setClientName("tokenClient1");
        client.setId("tokenClientId");
        token1.setClient(client);

        CouchDBToken token2 = new CouchDBToken();
        token2.setId("tokenId2");
        token2.setTokenName("tokenName2");
        token2.setClient(client);
        em.persist(token1);
        em.persist(token2);

        String queryWithoutClause = "Select t from CouchDBToken t";
        assertOnTokenScroll(queryWithoutClause, 2);

        String queryWithClause = "Select t from CouchDBToken t where t.tokenName='tokenName1'";

        assertOnTokenScroll(queryWithClause, 1);

        // TODO:: Need to discuss with KK, this should be working with token
        // support. Special scenario.
        String queryWithIdClause = "Select t from CouchDBToken t where t.id = 'tokenId1'";
        //
        assertOnTokenScroll(queryWithIdClause, 1);

    }

    private void assertOnTokenScroll(String queryClause, int expected)
    {
        Query query = (Query) em.createQuery(queryClause, CouchDBToken.class);

        int count = 0;
        Iterator<CouchDBToken> tokens = query.iterate();
        while (tokens.hasNext())
        {
            CouchDBToken token = tokens.next();
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
        PersonCouchDB p1 = new PersonCouchDB();
        p1.setPersonId("1");
        p1.setPersonName("vivek");
        p1.setAge(10);
        p1.setDay(Day.THURSDAY);
        p1.setMonth(Month.APRIL);

        PersonCouchDB p2 = new PersonCouchDB();
        p2.setPersonId("2");
        p2.setPersonName("vivek");
        p2.setAge(20);
        p2.setDay(Day.THURSDAY);
        p2.setMonth(Month.APRIL);

        PersonCouchDB p3 = new PersonCouchDB();
        p3.setPersonId("3");
        p3.setPersonName("vivek");
        p3.setAge(15);
        p3.setDay(Day.THURSDAY);
        p3.setMonth(Month.APRIL);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.flush();
        em.clear();
        final String queryWithoutClause = "Select p from PersonCouchDB p";

        assertOnScroll(queryWithoutClause, 3);

        final String queryWithClause = "Select p from PersonCouchDB p where p.personName = vivek";

        assertOnScroll(queryWithClause, 3);

        final String queryWithAndClause = "Select p from PersonCouchDB p where p.personName = vivek and p.age = 15";

        assertOnScroll(queryWithAndClause, 1);

        final String queryWithLTClause = "Select p from PersonCouchDB p where p.personName = vivek and p.age < 15";

        assertOnScroll(queryWithLTClause, 1);

        // Don't uncomment it, because only one and clause is supported as of
        // now.

        // final String queryWithGTClause =
        // "Select p from PersonCouchDB p where p.personName = vivek and p.age >= 15";
        //
        // assertOnScroll(queryWithGTClause, 2);
        //
        // final String queryWithLTGTClause =
        // "Select p from PersonCouchDB p where p.personName = vivek and p.age > 10 and p.age < 20";
        //
        // assertOnScroll(queryWithLTGTClause, 1);
        //
        // final String queryWithLTGTEClause =
        // "Select p from PersonCouchDB p where p.personName = vivek and p.age >= 10 and p.age < 20";
        //
        // assertOnScroll(queryWithLTGTEClause, 2);

        String queryWithIdClause = "Select p from PersonCouchDB p where p.personId = '2' ";
        assertOnScroll(queryWithIdClause, 1);
    }

    private void assertOnScroll(final String queryWithoutClause, int expectedCount)
    {
        Query query = (Query) em.createQuery(queryWithoutClause, PersonCouchDB.class);

        assertOnFetch(query, 0, expectedCount);
        assertOnFetch(query, 2, expectedCount); // less records

        assertOnFetch(query, 4, expectedCount); // more fetch size than
                                                // available in db.
        assertOnFetch(query, 3, expectedCount); // more fetch size than
                                                // available in db.

        assertOnFetch(query, null, expectedCount); // set to null;

    }

    private void assertOnFetch(Query query, Integer fetchSize, int expectedCount)
    {
        query.setFetchSize(fetchSize);
        int counter = 0;
        Iterator<PersonCouchDB> iter = query.iterate();

        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            counter++;
        }

        Assert.assertEquals(counter, fetchSize == null || expectedCount < fetchSize ? expectedCount : fetchSize);
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
        emf.close();
        emf = null;
        super.dropDatabase();
    }
}
