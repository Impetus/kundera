/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.Map;

import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * Test case for {@link ObjectGraphBuilder}
 * 
 * @author amresh.singh
 */
public class ObjectGraphBuilderTest
{
    private ObjectGraphBuilder graphBuilder;

    private String _persistenceUnit = "kunderatest";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        EntityManagerFactoryImpl emfImpl = getEntityManagerFactory();
        new PersistenceUnitConfiguration(null, emfImpl.getKunderaMetadataInstance(), "kunderatest").configure();
        PersistenceCache persistenceCache = new PersistenceCache();

        graphBuilder = new ObjectGraphBuilder(persistenceCache, new PersistenceDelegator(
                emfImpl.getKunderaMetadataInstance(), persistenceCache));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.graph.ObjectGraphBuilder#getObjectGraph(java.lang.Object)}
     * .
     */
    @Test
    public void testGetObjectGraph()
    {
        Store store = new Store(1, "Food Bazaar, Noida");
        store.addCounter(new BillingCounter(1, "A"));
        store.addCounter(new BillingCounter(2, "B"));
        store.addCounter(new BillingCounter(3, "C"));

        ObjectGraph graph = graphBuilder.getObjectGraph(store, null);

        Assert.assertNotNull(graph);
        Node headNode = graph.getHeadNode();
        Map<String, Node> nodeMappings = graph.getNodeMapping();

        Assert.assertNotNull(headNode);
        Assert.assertNotNull(nodeMappings);
        Assert.assertFalse(nodeMappings.isEmpty());
        Assert.assertEquals(4, nodeMappings.size());

        Assert.assertTrue(headNode.getParents() == null);
        Assert.assertEquals(3, headNode.getChildren().size());
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(_persistenceUnit);
    }
}
