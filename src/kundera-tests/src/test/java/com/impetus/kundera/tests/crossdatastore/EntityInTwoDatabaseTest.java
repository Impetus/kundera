package com.impetus.kundera.tests.crossdatastore;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.tests.entities.CommonUser;

public class EntityInTwoDatabaseTest
{

    private EntityManagerFactory emfMongo;

    private EntityManagerFactory emfCouchdb;

    @Before
    public void setUp() throws Exception
    {
        emfMongo = Persistence.createEntityManagerFactory("commonMongo");

        emfCouchdb = Persistence.createEntityManagerFactory("commonCouchdb");
    }

    @Test
    public void test()
    {

        CommonUser user1 = new CommonUser();
        user1.setFirstName("Kuldeep");
        user1.setUserId("1");
        user1.setLastName("Mishra");

        CommonUser user2 = new CommonUser();
        user2.setFirstName("Vivek");
        user2.setUserId("2");
        user2.setLastName("Mishra");

        EntityManager emMongo = emfMongo.createEntityManager();

        EntityManager emCouchdb = emfCouchdb.createEntityManager();

        emMongo.persist(user1);
        emCouchdb.persist(user2);

        emMongo.clear();
        emCouchdb.clear();

        CommonUser foundUserMongo = emMongo.find(CommonUser.class, "1");

        CommonUser foundUserCouchdb = emCouchdb.find(CommonUser.class, "2");

        Assert.assertNotNull(foundUserCouchdb);
        Assert.assertEquals("2", foundUserCouchdb.getUserId());
        Assert.assertEquals("Mishra", foundUserCouchdb.getLastName());
        Assert.assertEquals("Vivek", foundUserCouchdb.getFirstName());
        Assert.assertNotNull(foundUserMongo);
        Assert.assertEquals("1", foundUserMongo.getUserId());
        Assert.assertEquals("Mishra", foundUserMongo.getLastName());
        Assert.assertEquals("Kuldeep", foundUserMongo.getFirstName());

        foundUserCouchdb.setFirstName("KK");

        foundUserMongo.setFirstName("vivs");

        emCouchdb.merge(foundUserCouchdb);
        emMongo.merge(foundUserMongo);

        emMongo.clear();
        emCouchdb.clear();

        CommonUser updatedUserMongo = emMongo.find(CommonUser.class, "1");

        CommonUser updatedUserCouchdb = emCouchdb.find(CommonUser.class, "2");

        Assert.assertNotNull(updatedUserCouchdb);
        Assert.assertEquals("2", updatedUserCouchdb.getUserId());
        Assert.assertEquals("Mishra", updatedUserCouchdb.getLastName());
        Assert.assertEquals("KK", updatedUserCouchdb.getFirstName());
        Assert.assertNotNull(updatedUserMongo);
        Assert.assertEquals("1", updatedUserMongo.getUserId());
        Assert.assertEquals("Mishra", updatedUserMongo.getLastName());
        Assert.assertEquals("vivs", updatedUserMongo.getFirstName());

        emMongo.remove(updatedUserMongo);
        emCouchdb.remove(updatedUserCouchdb);

        CommonUser deletedUserMongo = emMongo.find(CommonUser.class, "1");

        CommonUser deletedUserCouchdb = emCouchdb.find(CommonUser.class, "2");

        Assert.assertNull(deletedUserMongo);
        Assert.assertNull(deletedUserCouchdb);

    }

    @After
    public void tearDown() throws Exception
    {
        emfMongo.close();
        emfCouchdb.close();
    }
}
