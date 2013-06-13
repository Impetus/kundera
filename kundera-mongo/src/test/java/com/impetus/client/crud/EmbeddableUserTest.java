package com.impetus.client.crud;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;

public class EmbeddableUserTest
{

    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setUp()
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
    }

    @Test
    public void test()
    {
        AppUser user = new AppUser();
        user.setId("id");
        UserProperties properties = new UserProperties();
        user.setPropertyContainer(properties);
        em.persist(user);

        em.clear();

        AppUser result = em.find(AppUser.class, "id");

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getPropertyKeys());
        Assert.assertFalse(result.getPropertyKeys().isEmpty());
        Assert.assertEquals(1, result.getPropertyKeys().size());
        Assert.assertNotNull(result.getPropertyValues());
        Assert.assertFalse(result.getPropertyValues().isEmpty());
        Assert.assertEquals(1, result.getPropertyValues().size());
        Assert.assertNotNull(result.getSearchList());
        Assert.assertTrue(result.getSearchList().isEmpty());
        Assert.assertNotNull(result.getTags());
        Assert.assertFalse(result.getTags().isEmpty());
        Assert.assertEquals(2, result.getTags().size());

        UserProperties propertyContainer = result.getPropertyContainer();
        Assert.assertNotNull(propertyContainer);
        Assert.assertEquals("hello", propertyContainer.getHi());
        Assert.assertNotNull(propertyContainer.getPropertyKeys());
        Assert.assertFalse(propertyContainer.getPropertyKeys().isEmpty());
        Assert.assertEquals(1, propertyContainer.getPropertyKeys().size());
        Assert.assertNotNull(propertyContainer.getPropertyValues());
        Assert.assertFalse(propertyContainer.getPropertyValues().isEmpty());
        Assert.assertEquals(1, propertyContainer.getPropertyValues().size());
        Assert.assertNotNull(propertyContainer.getTags());
        Assert.assertFalse(propertyContainer.getTags().isEmpty());
        Assert.assertEquals(1, propertyContainer.getTags().size());

    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }
}
