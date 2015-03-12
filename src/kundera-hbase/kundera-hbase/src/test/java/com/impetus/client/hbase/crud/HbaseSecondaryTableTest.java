package com.impetus.client.hbase.crud;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

public class HbaseSecondaryTableTest
{

    private EntityManagerFactory emf;
    
    /** The em. */
    private static EntityManager em;
    
    private HBaseCli cli;

    @Before
    public void setUp() throws Exception
    {
        
        cli = new HBaseCli();
        cli.startCluster();
      
        emf = Persistence.createEntityManagerFactory("hbaseTest");
        em = emf.createEntityManager();
     
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        cli.dropTable("KunderaExamples");
        cli.stopCluster("KunderaExamples");
       
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("hbaseTest"));
        emf.close();
    }

    @Test
    public void test()
    {
        EntityManager em = emf.createEntityManager();
        cli.addColumnFamily("KunderaExamples", "HBASE_SECONDARY_TABLE");
        cli.addColumnFamily("KunderaExamples", "t_country");
        

        EmbeddedEntity embeddedEntity = new EmbeddedEntity();
        embeddedEntity.setEmailId("kuldeep.mishra@gmail.com");
        embeddedEntity.setPhoneNo(9512345346l);
        
        List<EmbeddedCollectionEntity> embeddedEntities = new ArrayList<EmbeddedCollectionEntity>();
        
        EmbeddedCollectionEntity collectionEntity1 = new EmbeddedCollectionEntity();
        collectionEntity1.setCollectionId("c1");
        collectionEntity1.setCollectionName("Collection 1");
        embeddedEntities.add(collectionEntity1);
        
        EmbeddedCollectionEntity collectionEntity2 = new EmbeddedCollectionEntity();
        collectionEntity2.setCollectionId("c2");
        collectionEntity2.setCollectionName("Collection 2");
        embeddedEntities.add(collectionEntity2);
       
        HbaseSecondaryTableEntity entity = new HbaseSecondaryTableEntity();
        entity.setAge(24);
        entity.setObjectId("123");
        entity.setName("Kuldeep");
        entity.setEmbeddedEntity(embeddedEntity);
        entity.setCountry("Canada");
        entity.setEmbeddedEntities(embeddedEntities);
       
        
        PersonSecondaryTableAddress address = new PersonSecondaryTableAddress(12.23);
        address.setAddress("india");
        entity.setAddress(address);

        em.persist(entity);
        


        em.clear();

        HbaseSecondaryTableEntity foundEntity = em.find(HbaseSecondaryTableEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("Kuldeep", foundEntity.getName());
        Assert.assertEquals(24, foundEntity.getAge());
        Assert.assertEquals("Canada", foundEntity.getCountry());
        Assert.assertNotNull(foundEntity.getEmbeddedEntity());
        Assert.assertEquals("kuldeep.mishra@gmail.com", foundEntity.getEmbeddedEntity().getEmailId());
        Assert.assertEquals(9512345346l, foundEntity.getEmbeddedEntity().getPhoneNo());
        Assert.assertNotNull(foundEntity.getAddress());
        Assert.assertEquals("india",foundEntity.getAddress().getAddress());
        Assert.assertEquals(2, foundEntity.getEmbeddedEntities().size());
        Assert.assertEquals("Collection 1", foundEntity.getEmbeddedEntities().get(0).getCollectionName());
        Assert.assertEquals("Collection 2", foundEntity.getEmbeddedEntities().get(1).getCollectionName());
       
        foundEntity.setAge(25);
        foundEntity.setName("kk");
        foundEntity.getEmbeddedEntity().setEmailId("kuldeep.mishra@yahoo.com");

        em.merge(foundEntity);

        em.clear();

        foundEntity = em.find(HbaseSecondaryTableEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("kk", foundEntity.getName());
        Assert.assertEquals(25, foundEntity.getAge());
        Assert.assertNotNull(foundEntity.getEmbeddedEntity());
        Assert.assertEquals("kuldeep.mishra@yahoo.com", foundEntity.getEmbeddedEntity().getEmailId());
        Assert.assertEquals(9512345346l, foundEntity.getEmbeddedEntity().getPhoneNo());

        em.remove(foundEntity);

        foundEntity = em.find(HbaseSecondaryTableEntity.class, "123");
        Assert.assertNull(foundEntity);

    }
    
    

}
