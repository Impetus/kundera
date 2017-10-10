/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.crud.compositeType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.query.Query;

/**
 * @author Kuldeep.Mishra
 * 
 */
public class EntityWithMultiplePartitionKeyTest
{

    protected static String _PU = "compositedatatype";

    protected EntityManagerFactory emf;

    private static final String _keyspace = "CompositeCassandra";

    /** The Constant logger. */
    private static final Logger log = LoggerFactory.getLogger(EntityWithMultiplePartitionKeyTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.createKeySpace(_keyspace);
        try
        {
            CassandraCli
                    .executeCqlQuery(
                            "create table \"EntityWithMultiplePartitionKey\"(\"partitionKey1\" text, \"partitionKey2\" int, \"clusterkey1\" text, \"clusterkey2\" int, \"entityDiscription\" text, action text, PRIMARY KEY((\"partitionKey1\", \"partitionKey2\"), \"clusterkey1\", \"clusterkey2\"))",
                            _keyspace);
        }
        catch (Exception e)
        {
            log.warn(e.getMessage());
        }
        Map<String, String> propertymap = new HashMap<String, String>();
        propertymap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "");
        emf = Persistence.createEntityManagerFactory(_PU, propertymap);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace(_keyspace);
    }

    @Test
    public void testCRUD()
    {
        EntityManager em = emf.createEntityManager();

        PartitionKey partitionKey = new PartitionKey();
        partitionKey.setPartitionKey1("partitionKey1");
        partitionKey.setPartitionKey2(1);

        IdWithMultiplePartitionKey id = new IdWithMultiplePartitionKey();
        id.setClusterkey1("clusterkey1");
        id.setClusterkey2(11);
        id.setPartitionKey(partitionKey);

        EntityWithMultiplePartitionKey entity = new EntityWithMultiplePartitionKey();
        entity.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity.setAction("Persisting");
        entity.setId(id);

        // Insert.
        em.persist(entity);

        em.clear();

        // Retrieve.
        EntityWithMultiplePartitionKey foundEntity = em.find(EntityWithMultiplePartitionKey.class, id);
        Assert.assertNotNull(foundEntity);
        Assert.assertNotNull(foundEntity.getId());
        Assert.assertNotNull(foundEntity.getId().getPartitionKey());
        Assert.assertEquals("Persisting", foundEntity.getAction());
        Assert.assertEquals("Entity to test composite key with multiple partition key.",
                foundEntity.getEntityDiscription());
        Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
        Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
        Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
        Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());

        foundEntity.setAction("updating");
        // Update.
        em.merge(foundEntity);

        em.clear();

        foundEntity = em.find(EntityWithMultiplePartitionKey.class, id);
        Assert.assertNotNull(foundEntity);
        Assert.assertNotNull(foundEntity.getId());
        Assert.assertNotNull(foundEntity.getId().getPartitionKey());
        Assert.assertEquals("updating", foundEntity.getAction());
        Assert.assertEquals("Entity to test composite key with multiple partition key.",
                foundEntity.getEntityDiscription());
        Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
        Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
        Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
        Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());

        // Delete.
        em.remove(foundEntity);
        em.clear();

        foundEntity = em.find(EntityWithMultiplePartitionKey.class, id);
        Assert.assertNull(foundEntity);

    }

    @Test
    public void testQuery()
    {
        EntityManager em = emf.createEntityManager();

        PartitionKey partitionKey1 = new PartitionKey();
        partitionKey1.setPartitionKey1("partitionKey1");
        partitionKey1.setPartitionKey2(1);

        IdWithMultiplePartitionKey id1 = new IdWithMultiplePartitionKey();
        id1.setClusterkey1("clusterkey1");
        id1.setClusterkey2(11);
        id1.setPartitionKey(partitionKey1);

        EntityWithMultiplePartitionKey entity1 = new EntityWithMultiplePartitionKey();
        entity1.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity1.setAction("Persisting");
        entity1.setId(id1);

        PartitionKey partitionKey2 = new PartitionKey();
        partitionKey2.setPartitionKey1("partitionKey2");
        partitionKey2.setPartitionKey2(2);

        IdWithMultiplePartitionKey id2 = new IdWithMultiplePartitionKey();
        id2.setClusterkey1("clusterkey2");
        id2.setClusterkey2(111);
        id2.setPartitionKey(partitionKey2);

        EntityWithMultiplePartitionKey entity2 = new EntityWithMultiplePartitionKey();
        entity2.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity2.setAction("Persisting");
        entity2.setId(id2);

        PartitionKey partitionKey3 = new PartitionKey();
        partitionKey3.setPartitionKey1("partitionKey3");
        partitionKey3.setPartitionKey2(3);

        IdWithMultiplePartitionKey id3 = new IdWithMultiplePartitionKey();
        id3.setClusterkey1("clusterkey3");
        id3.setClusterkey2(1111);
        id3.setPartitionKey(partitionKey3);

        EntityWithMultiplePartitionKey entity3 = new EntityWithMultiplePartitionKey();
        entity3.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity3.setAction("Persisting");
        entity3.setId(id3);

        // Insert.
        em.persist(entity1);
        em.persist(entity2);
        em.persist(entity3);

        em.clear();

        // Select All query.
        List<EntityWithMultiplePartitionKey> foundEntitys = em.createQuery(
                "select e from EntityWithMultiplePartitionKey e").getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(3, foundEntitys.size());

        int count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());

            if (foundEntity.getId().getClusterkey1().equals("clusterkey1"))
            {
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            else if (foundEntity.getId().getClusterkey1().equals("clusterkey2"))
            {
                Assert.assertEquals(111, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey2", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(2, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            else
            {
                Assert.assertEquals("clusterkey3", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(1111, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey3", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(3, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.clear();

        // Select by both partition key query.
        foundEntitys = em
                .createQuery(
                        "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1 and e.id.partitionKey.partitionKey2=1")
                .getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        // Select by both partition key query and one cluster key.
        foundEntitys = em
                .createQuery(
                        "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1 and e.id.partitionKey.partitionKey2=1 and e.id.clusterkey1=clusterkey1")
                .getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        // Select by both partition key query.
        foundEntitys = em
                .createQuery("select e from EntityWithMultiplePartitionKey e where e.id.partitionKey = :partitionKey")
                .setParameter("partitionKey", partitionKey1).getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        // Select by first partition key with equal clause and second partition
        // key with IN clause.
        try
        {
            foundEntitys = em
                    .createQuery(
                            "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1 and e.id.partitionKey.partitionKey2 IN (1, 2, 3)")
                    .getResultList();

            Assert.assertNotNull(foundEntitys);
            Assert.assertFalse(foundEntitys.isEmpty());
            Assert.assertEquals(1, foundEntitys.size());

            count = 0;
            for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
            {
                Assert.assertNotNull(foundEntity);
                Assert.assertNotNull(foundEntity.getId());
                Assert.assertNotNull(foundEntity.getId().getPartitionKey());
                Assert.assertEquals("Persisting", foundEntity.getAction());
                Assert.assertEquals("Entity to test composite key with multiple partition key.",
                        foundEntity.getEntityDiscription());
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            Assert.assertEquals(1, count);
        }
        catch (Exception e)
        {
            log.warn(e.getMessage());
        }

        // Select by composite id object query.
        try
        {
            foundEntitys = em.createQuery("select e from EntityWithMultiplePartitionKey e where e.id = :id")
                    .setParameter("id", id1).getResultList();

            Assert.assertNotNull(foundEntitys);
            Assert.assertFalse(foundEntitys.isEmpty());
            Assert.assertEquals(1, foundEntitys.size());

            count = 0;
            for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
            {
                Assert.assertNotNull(foundEntity);
                Assert.assertNotNull(foundEntity.getId());
                Assert.assertNotNull(foundEntity.getId().getPartitionKey());
                Assert.assertEquals("Persisting", foundEntity.getAction());
                Assert.assertEquals("Entity to test composite key with multiple partition key.",
                        foundEntity.getEntityDiscription());
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            Assert.assertEquals(1, count);
        }
        catch (Exception e)
        {
            Assert.fail();
        }
        
     // Select by cluster key only.
        try
        {
            foundEntitys = em.createQuery("select e from EntityWithMultiplePartitionKey e where e.id.clusterkey1=clusterkey1").getResultList();

            Assert.assertNotNull(foundEntitys);
            Assert.assertFalse(foundEntitys.isEmpty());
            Assert.assertEquals(1, foundEntitys.size());

            count = 0;
            for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
            {
                Assert.assertNotNull(foundEntity);
                Assert.assertNotNull(foundEntity.getId());
                Assert.assertNotNull(foundEntity.getId().getPartitionKey());
                Assert.assertEquals("Persisting", foundEntity.getAction());
                Assert.assertEquals("Entity to test composite key with multiple partition key.",
                        foundEntity.getEntityDiscription());
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            Assert.assertEquals(1, count);
        }
        catch (Exception e)
        {
            Assert.fail();
        }

        // Select by first partition key query.
        try
        {
            foundEntitys = em
                    .createQuery(
                            "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1")
                    .getResultList();
            EntityWithMultiplePartitionKey foundEntity = foundEntitys.get(0);
			Assert.assertNotNull(foundEntity );
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());

        }
        catch (Exception e)
        {
        	Assert.fail();
        }

        //test iteration of result.
        Query query = (com.impetus.kundera.query.Query) em
                .createQuery("SELECT e FROM EntityWithMultiplePartitionKey e");

        query.setFetchSize(1);

        Iterator<EntityWithMultiplePartitionKey> iterator = query.iterate();
        count = 0;
        while (iterator.hasNext())
        {
            EntityWithMultiplePartitionKey entity = iterator.next();
            Assert.assertNotNull(entity);
            Assert.assertNotNull(entity.getId());
            Assert.assertNotNull(entity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", entity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    entity.getEntityDiscription());
            count++;
        }
        Assert.assertEquals(1, count);
    }

}
