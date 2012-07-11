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
package com.impetus.kundera.persistence.context;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.configure.Configurator;
import com.impetus.kundera.configure.MetamodelConfiguration;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.graph.Store;

/**
 * Test case for {@link PersistenceCache}
 * 
 * @author amresh.singh
 */
public class PersistenceCacheTest
{

    PersistenceCache pc;

    FlushManager flushManager;

    ObjectGraphBuilder graphBuilder;

//    Configurator configurator = new Configurator("kunderatest");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        flushManager = new FlushManager();
        graphBuilder = new ObjectGraphBuilder(pc);

//        configurator.configure();
        new PersistenceUnitConfiguration("kunderatest").configure();
        new MetamodelConfiguration("kunderatest").configure();        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testPersistenceCache()
    {
        Store store = new Store(1, "Food Bazaar, Noida");
        store.addCounter(new BillingCounter(1, "A"));
        store.addCounter(new BillingCounter(2, "B"));
        store.addCounter(new BillingCounter(3, "C"));

        ObjectGraph graph = graphBuilder.getObjectGraph(store, null);

        pc.getMainCache().addGraphToCache(graph, pc);

        Assert.assertNotNull(pc.getMainCache());
        Assert.assertEquals(1, pc.getMainCache().getHeadNodes().size());

        Node headNode = pc.getMainCache().getNodeFromCache(ObjectGraphUtils.getNodeId("1", Store.class));

        Assert.assertNull(headNode.getParents());
        Assert.assertEquals(3, headNode.getChildren().size());

        Assert.assertEquals(4, pc.getMainCache().size());
    }
}
