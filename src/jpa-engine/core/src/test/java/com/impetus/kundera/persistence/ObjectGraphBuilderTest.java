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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.client.CoreTestClientFactory;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
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
        getEntityManagerFactory();
        new PersistenceUnitConfiguration("kunderatest").configure();
        PersistenceCache persistenceCache = new PersistenceCache();

        graphBuilder = new ObjectGraphBuilder(persistenceCache, new PersistenceDelegator(persistenceCache));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
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
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        Map<String, Object> props = new HashMap<String, Object>();

        props.put(Constants.PERSISTENCE_UNIT_NAME, _persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, CoreTestClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaTest");

        List<String> pus = new ArrayList<String>();
        pus.add(_persistenceUnit);
        clazzToPu.put(Store.class.getName(), pus);
        clazzToPu.put(BillingCounter.class.getName(), pus);

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(_persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(_persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        
        appMetadata.setClazzToPuMap(clazzToPu);

        KunderaMetadata.INSTANCE.setApplicationMetadata(appMetadata);
        
        MetadataBuilder metadataBuilder = new MetadataBuilder(_persistenceUnit, CoreTestClient.class.getSimpleName(), null);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(Store.class, metadataBuilder.buildEntityMetadata(Store.class));
        metaModel.addEntityMetadata(BillingCounter.class, metadataBuilder.buildEntityMetadata(BillingCounter.class));

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);
        return null;
    }
}
