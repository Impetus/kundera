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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.PersonRDBMS;
import com.impetus.client.crud.entities.RDBMSToken;
import com.impetus.client.crud.entities.RDBMSTokenClient;
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

    private RDBMSCli cli;

    private static final String SCHEMA = "testdb";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        createSchema();
        
        emf = Persistence.createEntityManagerFactory("testHibernate");
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
        RDBMSTokenClient client = new RDBMSTokenClient();
        client.setClientName("tokenClient1");
        client.setId("tokenClientId");

        RDBMSToken token1 = new RDBMSToken();
        token1.setId("RdbmsTokenId1");
        token1.setTokenName("tokenName1");
        token1.setClient(client);

        RDBMSToken token2 = new RDBMSToken();
        token2.setId("tokenId2");
        token2.setTokenName("tokenName2");
        token2.setClient(client);

        em.persist(token1);
        em.persist(token2);

        String queryWithoutClause = "Select t from RDBMSToken t";
        assertOnTokenScroll(queryWithoutClause, 2);

        String queryWithClause = "Select t from RDBMSToken t where t.tokenName='tokenName1'";

        assertOnTokenScroll(queryWithClause, 1);

        // TODO:: Need to discuss , this should be working with token
        // support. Special scenario.
        String queryWithIdClause = "Select t from RDBMSToken t where t.tokenId = 'RDBMSTokenId1'";
        //        assertOnTokenScroll(queryWithIdClause, 1);

    }

    private void assertOnTokenScroll(String queryClause, int expected)
    {
        Query query = (Query) em.createQuery(queryClause, RDBMSToken.class);

        int count = 0;
        Iterator<RDBMSToken> tokens = query.iterate();
        while (tokens.hasNext())
        {
            RDBMSToken token = tokens.next();
            Assert.assertNotNull(token);
            RDBMSTokenClient client = token.getClient();
            Assert.assertNotNull(client);
            Assert.assertEquals("tokenClient1", client.getClientName());
            // Assert.assertEquals(2, client.getTokens().size());
            count++;
        }

        Assert.assertTrue(count > 0);
        Assert.assertTrue(count == expected);
    }

    private void onScroll()
    {
        Object p1 = prepareRDBMSInstance("1", 10);
        Object p2 = prepareRDBMSInstance("2", 20);
        Object p3 = prepareRDBMSInstance("3", 15);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.flush();
        em.clear();
        final String queryWithoutClause = "Select p from PersonRDBMS p";

        assertOnScroll(queryWithoutClause, 3);

        final String queryWithClause = "Select p from PersonRDBMS p where p.personName = vivek";

        assertOnScroll(queryWithClause, 3);

        final String queryWithAndClause = "Select p from PersonRDBMS p where p.personName = vivek and p.age = 15";

        assertOnScroll(queryWithAndClause, 1);

        final String queryWithLTClause = "Select p from PersonRDBMS p where p.personName = vivek and p.age < 15";

        assertOnScroll(queryWithLTClause, 1);

        final String queryWithGTClause = "Select p from PersonRDBMS p where p.personName = vivek and p.age >= 15";

        assertOnScroll(queryWithGTClause, 2);

        final String queryWithLTGTClause = "Select p from PersonRDBMS p where p.personName = vivek and p.age > 10 and p.age < 20";

        assertOnScroll(queryWithLTGTClause, 1);

        final String queryWithLTGTEClause = "Select p from PersonRDBMS p where p.personName = vivek and p.age >= 10 and p.age < 20";

        assertOnScroll(queryWithLTGTEClause, 2);

        String queryWithIdClause = "Select p from PersonRDBMS p where p.personId = '2' ";
        assertOnScroll(queryWithIdClause, 1);
    }

    private void assertOnScroll(final String queryWithoutClause, int expectedCount)
    {
        Query query = (Query) em.createQuery(queryWithoutClause, PersonRDBMS.class);

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
        Iterator<PersonRDBMS> iter = query.iterate();

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
        em.close();
        emf.close();
        dropSchema();
    }

    private void createSchema() throws SQLException
    {
        try
        {
            cli = new RDBMSCli(SCHEMA);
            cli.createSchema(SCHEMA);
            cli.update("CREATE MEMORY TABLE TESTDB.PERSON (PERSON_ID VARCHAR(90) PRIMARY KEY, PERSON_NAME VARCHAR(256), AGE INTEGER)");
            cli.update("CREATE MEMORY TABLE TESTDB.TOKENS (TOKEN_ID VARCHAR(90) PRIMARY KEY, TOKEN_NAME VARCHAR(256), CLIENT_ID VARCHAR(256))");
            cli.update("CREATE MEMORY TABLE TESTDB.CLIENT (CLIENT_ID VARCHAR(90) PRIMARY KEY, CLIENT_NAME VARCHAR(256))");
        }
        catch (Exception e)
        {

            cli.update("DELETE FROM TESTDB.PERSON");
            cli.update("DROP TABLE TESTDB.PERSON");
            cli.update("DELETE FROM TESTDB.TOKENS");
            cli.update("DROP TABLE TESTDB.TOKENS");
            cli.update("DELETE FROM TESTDB.CLIENT");
            cli.update("DROP TABLE TESTDB.CLIENT");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE MEMORY TABLE TESTDB.PERSON (PERSON_ID VARCHAR(90) PRIMARY KEY, PERSON_NAME VARCHAR(256), AGE INTEGER)");
            cli.update("CREATE MEMORY TABLE TESTDB.TOKENS (TOKEN_ID VARCHAR(90) PRIMARY KEY, TOKEN_NAME VARCHAR(256), CLIENT_ID VARCHAR(256))");
            cli.update("CREATE MEMORY TABLE TESTDB.CLIENT (CLIENT_ID VARCHAR(90) PRIMARY KEY, CLIENT_NAME VARCHAR(256))");
            // nothing
            // do
        }
    }

    private void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.PERSON");
            cli.update("DROP TABLE TESTDB.PERSON");
            cli.update("DELETE FROM TESTDB.tokens");
            cli.update("DROP TABLE TESTDB.tokens");
            cli.update("DELETE FROM TESTDB.client");
            cli.update("DROP TABLE TESTDB.client");

            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
            cli.shutdown();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}