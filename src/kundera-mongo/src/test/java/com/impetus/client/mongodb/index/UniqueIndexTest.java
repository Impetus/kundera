package com.impetus.client.mongodb.index;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.utils.MongoUtils;

/**
 * The Class UniqueIndexTest.
 * 
 * @author devender.yadav
 */
public class UniqueIndexTest
{

    /** The Constant _PU. */
    private static final String PU = "mongo_pu";

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
     * Test CRUD GridFS.
     * 
     * @throws Exception
     *             the exceptiono
     */
    @Test
    public void testInsert() throws Exception
    {
        Book book1 = new Book("1", "title1", "cat1", 100);
        Book book2 = new Book("2", "title2", "cat2", 200);
        Book book3 = new Book("3", "title3", "cat2", 200);

        try
        {
            em.clear();
            em.persist(book1);

            Book book = em.find(Book.class, "1");
            Assert.assertNotNull(book);
            Assert.assertEquals("title1", book.getTitle());
            Assert.assertEquals("cat1", book.getCategory());
            Assert.assertEquals(100, book.getNumPages());

            em.clear();
            em.persist(book2);

            book = em.find(Book.class, "2");
            Assert.assertNotNull(book);
            Assert.assertEquals("title2", book.getTitle());
            Assert.assertEquals("cat2", book.getCategory());
            Assert.assertEquals(200, book.getNumPages());

            em.clear();
            em.persist(book3);
            Assert.fail();
        }
        catch (Exception ex)
        {
            Book b = em.find(Book.class, "3");
            Assert.assertNull(b);
        }
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
        MongoUtils.dropDatabase(emf, PU);
        emf.close();
    }

}
