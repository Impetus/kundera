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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * Test case for {@link PersistenceCache}
 * 
 * @author amresh.singh
 */
public class PersistenceCacheTest
{

    private PersistenceCache pc;

    private ObjectGraphBuilder graphBuilder;

    private String _persistenceUnit = "kunderatest";

    // Configurator configurator = new Configurator("kunderatest");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        graphBuilder = new ObjectGraphBuilder(pc);

        getEntityManagerFactory();
        // configurator.configure();
        new PersistenceUnitConfiguration("kunderatest").configure(null);
        // new MetamodelConfiguration("kunderatest").configure();
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

        Assert.assertNotNull(headNode);
        Assert.assertNull(headNode.getParents());
        Assert.assertEquals(3, headNode.getChildren().size());

        Assert.assertEquals(4, pc.getMainCache().size());
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
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(_persistenceUnit);
        clazzToPu.put(Store.class.getName(), pus);
        clazzToPu.put(BillingCounter.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(Store.class);
        EntityMetadata m1 = new EntityMetadata(BillingCounter.class);

        TableProcessor processor = new TableProcessor();
        processor.process(Store.class, m);
        processor.process(BillingCounter.class, m1);

        m.setPersistenceUnit(_persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(Store.class, m);
        metaModel.addEntityMetadata(BillingCounter.class, m1);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);
        return null;
    }
}
