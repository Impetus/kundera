package com.impetus.client.crud;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.junit.Assert;

public class SecondaryTableTestBase
{
    protected void testCRUD(EntityManagerFactory emf)
    {
        EntityManager em = emf.createEntityManager();

        EmbeddedEntity embeddedEntity = new EmbeddedEntity();
        embeddedEntity.setEmailId("kuldeep.mishra@gmail.com");
        embeddedEntity.setPhoneNo(9512345346l);

        SecondaryTableEntity entity = new SecondaryTableEntity();
        entity.setAge(24);
        entity.setObjectId("123");
        entity.setName("Kuldeep");
        entity.setEmbeddedEntity(embeddedEntity);

        em.persist(entity);

        em.clear();

        SecondaryTableEntity foundEntity = em.find(SecondaryTableEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("Kuldeep", foundEntity.getName());
        Assert.assertEquals(24, foundEntity.getAge());
        Assert.assertNotNull(foundEntity.getEmbeddedEntity());
        Assert.assertEquals("kuldeep.mishra@gmail.com", foundEntity.getEmbeddedEntity().getEmailId());
        Assert.assertEquals(9512345346l, foundEntity.getEmbeddedEntity().getPhoneNo());

        foundEntity.setAge(25);
        foundEntity.setName("kk");
        foundEntity.getEmbeddedEntity().setEmailId("kuldeep.mishra@yahoo.com");

        em.merge(foundEntity);

        em.clear();

        foundEntity = em.find(SecondaryTableEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("kk", foundEntity.getName());
        Assert.assertEquals(25, foundEntity.getAge());
        Assert.assertNotNull(foundEntity.getEmbeddedEntity());
        Assert.assertEquals("kuldeep.mishra@yahoo.com", foundEntity.getEmbeddedEntity().getEmailId());
        Assert.assertEquals(9512345346l, foundEntity.getEmbeddedEntity().getPhoneNo());

        em.remove(foundEntity);

        foundEntity = em.find(SecondaryTableEntity.class, "123");
        Assert.assertNull(foundEntity);
    }

}
