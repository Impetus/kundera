package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

public class MTOBiSelfAssociationTest
{

    private static final String SEC_IDX_CASSANDRA_TEST = "secIdxCassandraTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    protected Map propertyMap = null;

    protected boolean AUTO_MANAGE_SCHEMA = true;

    protected boolean USE_CQL = false;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");

        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            // loadData();
        }
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void test()
    {
        Group all = new Group();
        all.setResourceId("All"); // A
        all.setResourceName("resName");

        Group ungrouped = new Group();
        ungrouped.setResourceId("ungrouped");
        ungrouped.setParent(all); // B

        Group grouped = new Group();
        grouped.setResourceId("grouped");
        grouped.setParent(all); // C

        List<Group> children = new ArrayList<Group>();
        children.add(ungrouped);
        children.add(grouped);
        all.setChildren(children);

        em.persist(ungrouped);
        em.persist(grouped);
        em.persist(all);

        em.clear();
        Group parent = em.find(Group.class, "All");

        Assert.assertNotNull(parent);
        Assert.assertNotNull(parent.getChildren());
        Assert.assertNull(parent.getParent());
        Assert.assertEquals(2, parent.getChildren().size());
        Assert.assertSame(parent, parent.getChildren().iterator().next().getParent());
        Assert.assertSame(parent, parent.getChildren().iterator().next().getParent());

        em.clear();
        Group child1 = em.find(Group.class, "ungrouped");
        Assert.assertNotNull(child1);
        Assert.assertEquals(child1.getParent().getResourceId(), "All");
        Assert.assertNull(child1.getParent().getParent());
        Assert.assertEquals(child1.getResourceId(), "ungrouped");
        Assert.assertEquals(2, child1.getParent().getChildren().size());
        Assert.assertSame(child1.getParent().getChildren().iterator().next().getParent(), child1.getParent()
                .getChildren().iterator().next().getParent());

        em.clear();
        Group child2 = em.find(Group.class, "grouped");
        Assert.assertNotNull(child2);
        Assert.assertEquals(child2.getParent().getResourceId(), "All");
        Assert.assertNull(child2.getParent().getParent());
        Assert.assertEquals(child2.getResourceId(), "grouped");
        Assert.assertEquals(2, child2.getParent().getChildren().size());
        Assert.assertSame(child2.getParent().getChildren().iterator().next().getParent(), child2.getParent()
                .getChildren().iterator().next().getParent());

    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }
}
