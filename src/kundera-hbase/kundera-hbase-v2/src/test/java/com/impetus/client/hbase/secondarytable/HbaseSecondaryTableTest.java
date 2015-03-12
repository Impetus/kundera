/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.secondarytable;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class HbaseSecondaryTableTest.
 * 
 * @author Pragalbh Garg
 */
public class HbaseSecondaryTableTest
{
    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "secTableTest";

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

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
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
        em = emf.createEntityManager();
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
        emf.close();
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

    /**
     * Test.
     */
    @Test
    public void test()
    {
        EntityManager em = emf.createEntityManager();

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
        Assert.assertEquals("india", foundEntity.getAddress().getAddress());
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
