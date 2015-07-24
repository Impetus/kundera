/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.spark;

import java.nio.ByteBuffer;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.cassandra.utils.CassandraCli;
import com.impetus.client.spark.utils.SparkTestingUtils;

/**
 * The Class SparkCassClientTest.
 * 
 * @author: devender.yadav
 */
public class SparkCassClientTest
{

    /** The Constant PU. */
    private static final String PU = "spark_cass_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
        CassandraCli.cassandraSetUp("192.168.145.21", 9161);
        createKeyspace();
        createColumnFamily();
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Spark cass test.
     */
    @Test
    public void sparkCassTest()
    {
        persistBooks();
        testPersist();
        testQuery();
        testSaveIntermediateResult();
        intermediatePersistSyntaxTest();
    }

    /**
     * Persist books.
     */
    private void persistBooks()
    {
        Book book1 = new Book("1", "A Tale of Two Cities", "Charles Dickens", "History", 441);
        Book book2 = new Book("2", "The Lord of the Rings", "J. R. R. Tolkien", "Adventure", 1216);
        Book book3 = new Book("3", "The Da Vinci Code", "Dan Brown", "Thriller", 454);
        em.persist(book1);
        em.persist(book2);
        em.persist(book3);
    }

    /**
     * Test persist.
     */
    private void testPersist()
    {
        Book book = em.find(Book.class, "1");
        validateBook1(book);
        book = em.find(Book.class, "2");
        validateBook2(book);
        book = em.find(Book.class, "3");
        validateBook3(book);
    }

    /**
     * Query test.
     */
    private void testQuery()
    {
        List<Book> results = em.createNativeQuery("select * from spark_book").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, true, true, true);

        results = em.createNativeQuery("select * from spark_book where numPages > 450").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, false, true, true);

        results = em.createNativeQuery("select * from spark_book where title like 'The%'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, false, true, true);

        List aggregateResults = em.createNativeQuery("select sum(numPages) from spark_book").getResultList();
        Assert.assertNotNull(aggregateResults);
        Assert.assertEquals(1, aggregateResults.size());

        aggregateResults = em.createNativeQuery("select count(numPages) from spark_book group by numPages")
                .getResultList();
        Assert.assertNotNull(aggregateResults);
    }

    /**
     * Intermediate persist syntax test.
     */
    private void intermediatePersistSyntaxTest()
    {
        String sqlString = "INSERT INTO insert into cassandra.sparktest.table FROM (select * from spark_book)";
        Query query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO into cassandra.sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT asdf INTO cassandra.sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO dsfs cassandra.sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = " INTO cassandra.sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO .sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table.output FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table FROMM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table FROM sadsd (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table FROM ()";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "cassandra.sparktest.table FROM (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);

        sqlString = "INSERT INTO cassandra.sparktest.table (select * from spark_book)";
        query = em.createNativeQuery(sqlString, Book.class);
        exceptionTest(query);
    }

    /**
     * Save intermediate result.
     */
    private void testSaveIntermediateResult()
    {
        CassandraCli
                .executeCqlQuery(
                        "CREATE TABLE IF NOT EXISTS spark_book_copy (id text PRIMARY KEY, title text, author text, category text, \"numPages\" int)",
                        "sparktest");

        String sqlString = "INSERT INTO cassandra.sparktest.spark_book_copy FROM (select * from spark_book)";
        Query q = em.createNativeQuery(sqlString, Book.class);
        q.executeUpdate();

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_csv] AS CSV FROM (select * from spark_book)";
        q = em.createNativeQuery(sqlString, Book.class);
        q.executeUpdate();

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_json] AS JSON FROM (select * from spark_book)";
        q = em.createNativeQuery(sqlString, Book.class);
        q.executeUpdate();

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_csv");
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_json");
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        CassandraCli.executeCqlQuery("TRUNCATE spark_book", "sparktest");
        CassandraCli.executeCqlQuery("DROP TABLE spark_book", "sparktest");
        CassandraCli.executeCqlQuery("DROP KEYSPACE \"sparktest\"", "sparktest");
        emf.close();
        emf = null;
    }

    /**
     * Exception test.
     * 
     * @param query
     *            the query
     * @return true, if successful
     */
    private boolean exceptionTest(Query query)
    {
        try
        {
            query.executeUpdate();
        }
        catch (Exception e)
        {
            return true;
        }

        Assert.fail();
        return false;
    }

    /**
     * Validate book1.
     * 
     * @param book
     *            the book
     */
    protected void validateBook1(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals("A Tale of Two Cities", book.getTitle());
        Assert.assertEquals("Charles Dickens", book.getAuthor());
        Assert.assertEquals("History", book.getCategory());
        Assert.assertEquals(441, book.getNumPages());
    }

    /**
     * Validate book2.
     * 
     * @param book
     *            the book
     */
    protected void validateBook2(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals("The Lord of the Rings", book.getTitle());
        Assert.assertEquals("J. R. R. Tolkien", book.getAuthor());
        Assert.assertEquals("Adventure", book.getCategory());
        Assert.assertEquals(1216, book.getNumPages());
    }

    /**
     * Validate book3.
     * 
     * @param book
     *            the book
     */
    protected void validateBook3(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals("The Da Vinci Code", book.getTitle());
        Assert.assertEquals("Dan Brown", book.getAuthor());
        Assert.assertEquals("Thriller", book.getCategory());
        Assert.assertEquals(454, book.getNumPages());
    }

    /**
     * Assert results.
     * 
     * @param results
     *            the results
     * @param foundBook1
     *            the found book1
     * @param foundBook2
     *            the found book2
     * @param foundBook3
     *            the found book3
     */
    protected void assertResults(List<Book> results, boolean foundBook1, boolean foundBook2, boolean foundBook3)
    {
        for (Book book : results)
        {
            switch (book.getId())
            {
            case "1":
                if (foundBook1)
                    validateBook1(book);
                else
                    Assert.fail();
                break;
            case "2":
                if (foundBook2)
                    validateBook2(book);
                else
                    Assert.fail();
                break;
            case "3":
                if (foundBook3)
                    validateBook3(book);
                else
                    Assert.fail();
                break;
            }
        }
    }

    /**
     * Creates the column family.
     */
    private static void createColumnFamily()
    {
        try
        {
            CassandraCli.executeCqlQuery("USE \"sparktest\"", "sparktest");
            CassandraCli
                    .executeCqlQuery(
                            "CREATE TABLE IF NOT EXISTS spark_book (id text PRIMARY KEY, title text, author text, category text, \"numPages\" int)",
                            "sparktest");
        }
        catch (Exception e)
        {
        }
    }

    /**
     * Creates the keyspace.
     */
    private static void createKeyspace()
    {
        try
        {
            CassandraCli
                    .getClient()
                    .execute_cql3_query(
                            ByteBuffer.wrap("CREATE KEYSPACE \"sparktest\" WITH replication = {'class':'SimpleStrategy','replication_factor':3}"
                                    .getBytes("UTF-8")), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {

        }
    }
}
