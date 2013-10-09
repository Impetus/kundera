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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.client.CoreTestClientFactory;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.entity.album.AlbumBi_1_1_1_1;
import com.impetus.kundera.entity.album.AlbumUni_1_1_1_1;
import com.impetus.kundera.entity.album.AlbumUni_1_1_1_M;
import com.impetus.kundera.entity.album.AlbumUni_1_1_M_1;
import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.album.AlbumUni_1_M_M_M;
import com.impetus.kundera.entity.album.AlbumUni_M_1_1_M;
import com.impetus.kundera.entity.album.AlbumUni_M_M_1_1;
import com.impetus.kundera.entity.album.AlbumUni_M_M_M_M;
import com.impetus.kundera.entity.photo.PhotoBi_1_1_1_1;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_1_1;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_M_1;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_M_M;
import com.impetus.kundera.entity.photo.PhotoUni_M_1_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_M_M_1_1;
import com.impetus.kundera.entity.photo.PhotoUni_M_M_M_M;
import com.impetus.kundera.entity.photographer.PhotographerBi_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_M_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_M_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_1_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_M_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_M_M_M;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.context.EventLog.EventType;
import com.impetus.kundera.persistence.context.FlushManager;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * Test case for {@link FlushManager}
 * 
 * @author amresh.singh
 */
public class FlushStackManagerTest
{
    private PersistenceCache pc;

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
        pc = new PersistenceCache();
        graphBuilder = new ObjectGraphBuilder(pc, new PersistenceDelegator(pc));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }

    @Test
    public void testFlashStockForStore()
    {

        FlushManager flushManager = new FlushManager();

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

        markAllNodeAsDirty();
        flushManager.buildFlushStack(headNode, EventType.INSERT);

        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(4, fs.size());
    }

    @Test
    public void test_1_1_1_1()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_1_1_1_1 a = new PhotographerUni_1_1_1_1();
        a.setPhotographerId(1);
        AlbumUni_1_1_1_1 b = new AlbumUni_1_1_1_1();
        b.setAlbumId("b1");
        PhotoUni_1_1_1_1 c = new PhotoUni_1_1_1_1();
        c.setPhotoId("c1");
        a.setAlbum(b);
        b.setPhoto(c);

        ObjectGraph graph = graphBuilder.getObjectGraph(a, null);
        pc.getMainCache().addGraphToCache(graph, pc);

        Node headNode = pc.getMainCache().getNodeFromCache(ObjectGraphUtils.getNodeId("c1", PhotoUni_1_1_1_1.class));

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);

        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(3, fs.size());
    }

    @Test
    public void test_multi_1_1_1_1()
    {
        FlushManager flushManager = new FlushManager();

        PhotographerBi_1_1_1_1 a = new PhotographerBi_1_1_1_1();
        a.setPhotographerId(1);
        
        AlbumBi_1_1_1_1 b = new AlbumBi_1_1_1_1();
        b.setAlbumId("b1");
        
        PhotoBi_1_1_1_1 c = new PhotoBi_1_1_1_1();
        c.setPhotoId("c1");
        
        b.setPhotographer(a);
        b.setPhoto(c);

        ObjectGraph graph = graphBuilder.getObjectGraph(b, null);
        pc.getMainCache().addGraphToCache(graph, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);

        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(3, fs.size());
    }

    @Test
    public void test_1_1_1_M()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_1_1_1_M a = new PhotographerUni_1_1_1_M();
        a.setPhotographerId(1);
        AlbumUni_1_1_1_M b = new AlbumUni_1_1_1_M();
        b.setAlbumId("b1");
        PhotoUni_1_1_1_M c1 = new PhotoUni_1_1_1_M();
        c1.setPhotoId("c1");
        PhotoUni_1_1_1_M c2 = new PhotoUni_1_1_1_M();
        c2.setPhotoId("c2");
        PhotoUni_1_1_1_M c3 = new PhotoUni_1_1_1_M();
        c3.setPhotoId("c3");
        a.setAlbum(b);
        b.addPhoto(c1);
        b.addPhoto(c2);
        b.addPhoto(c3);

        ObjectGraph graph = graphBuilder.getObjectGraph(a, null);
        pc.getMainCache().addGraphToCache(graph, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(5, fs.size());
    }

    @Test
    public void test_1_1_M_1()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_1_1_M_1 a = new PhotographerUni_1_1_M_1();
        a.setPhotographerId(1);
        AlbumUni_1_1_M_1 b1 = new AlbumUni_1_1_M_1();
        b1.setAlbumId("b1");
        AlbumUni_1_1_M_1 b2 = new AlbumUni_1_1_M_1();
        b2.setAlbumId("b2");
        AlbumUni_1_1_M_1 b3 = new AlbumUni_1_1_M_1();
        b3.setAlbumId("b3");

        PhotoUni_1_1_M_1 c = new PhotoUni_1_1_M_1();
        c.setPhotoId("c");
        a.setAlbum(b1);
        b1.setPhoto(c);
        b2.setPhoto(c);
        b3.setPhoto(c);

        ObjectGraph graph = graphBuilder.getObjectGraph(a, null);
        ObjectGraph graphb2 = graphBuilder.getObjectGraph(b2, null);
        ObjectGraph graphb3 = graphBuilder.getObjectGraph(b3, null);

        pc.getMainCache().addGraphToCache(graph, pc);
        pc.getMainCache().addGraphToCache(graphb2, pc);
        pc.getMainCache().addGraphToCache(graphb3, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(3, fs.size());
        flushManager.clearFlushStack();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graphb2.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(2, fs.size());
        flushManager.clearFlushStack();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graphb3.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(2, fs.size());
        flushManager.clearFlushStack();
    }

    @Test
    public void test_1_M_1_M()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_1_M_1_M a = new PhotographerUni_1_M_1_M();
        a.setPhotographerId(1);
        AlbumUni_1_M_1_M b1 = new AlbumUni_1_M_1_M();
        b1.setAlbumId("b1");
        AlbumUni_1_M_1_M b2 = new AlbumUni_1_M_1_M();
        b2.setAlbumId("b2");

        PhotoUni_1_M_1_M c1 = new PhotoUni_1_M_1_M();
        c1.setPhotoId("c1");
        PhotoUni_1_M_1_M c2 = new PhotoUni_1_M_1_M();
        c2.setPhotoId("c2");
        PhotoUni_1_M_1_M c3 = new PhotoUni_1_M_1_M();
        c3.setPhotoId("c3");
        PhotoUni_1_M_1_M c4 = new PhotoUni_1_M_1_M();
        c4.setPhotoId("c4");

        b1.addPhoto(c1);
        b1.addPhoto(c2);
        b2.addPhoto(c3);
        b2.addPhoto(c4);
        a.addAlbum(b1);
        a.addAlbum(b2);

        ObjectGraph graph = graphBuilder.getObjectGraph(a, null);
        pc.getMainCache().addGraphToCache(graph, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(7, fs.size());
    }

    @Test
    public void test_1_M_M_M()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_1_M_M_M a = new PhotographerUni_1_M_M_M();
        a.setPhotographerId(1);
        AlbumUni_1_M_M_M b1 = new AlbumUni_1_M_M_M();
        b1.setAlbumId("b1");
        AlbumUni_1_M_M_M b2 = new AlbumUni_1_M_M_M();
        b2.setAlbumId("b2");

        PhotoUni_1_M_M_M c1 = new PhotoUni_1_M_M_M();
        c1.setPhotoId("c1");
        PhotoUni_1_M_M_M c2 = new PhotoUni_1_M_M_M();
        c2.setPhotoId("c2");
        PhotoUni_1_M_M_M c3 = new PhotoUni_1_M_M_M();
        c3.setPhotoId("c3");

        b1.addPhoto(c1);
        b1.addPhoto(c2);
        b2.addPhoto(c2);
        b2.addPhoto(c3);
        a.addAlbum(b1);
        a.addAlbum(b2);

        ObjectGraph graph = graphBuilder.getObjectGraph(a, null);
        pc.getMainCache().addGraphToCache(graph, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(6, fs.size());
    }

    @Test
    public void test_M_1_1_M()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_M_1_1_M a1 = new PhotographerUni_M_1_1_M();
        a1.setPhotographerId(1);
        PhotographerUni_M_1_1_M a2 = new PhotographerUni_M_1_1_M();
        a2.setPhotographerId(2);
        PhotographerUni_M_1_1_M a3 = new PhotographerUni_M_1_1_M();
        a3.setPhotographerId(3);

        AlbumUni_M_1_1_M b = new AlbumUni_M_1_1_M();
        b.setAlbumId("b");

        PhotoUni_M_1_1_M c1 = new PhotoUni_M_1_1_M();
        c1.setPhotoId("c1");
        PhotoUni_M_1_1_M c2 = new PhotoUni_M_1_1_M();
        c2.setPhotoId("c2");
        PhotoUni_M_1_1_M c3 = new PhotoUni_M_1_1_M();
        c3.setPhotoId("c3");

        b.addPhoto(c1);
        b.addPhoto(c2);
        b.addPhoto(c3);
        a1.setAlbum(b);

        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1, null);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2, null);
        ObjectGraph graph3 = graphBuilder.getObjectGraph(a3, null);

        pc.getMainCache().addGraphToCache(graph1, pc);

        markAllNodeAsDirty();

        flushManager.buildFlushStack(graph1.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(5, fs.size());
        flushManager.clearFlushStack();

        a2.setAlbum(b);
        pc.getMainCache().addGraphToCache(graph2, pc);
        markAllNodeAsDirty();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graph2.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(1, fs.size());
        flushManager.clearFlushStack();

        a3.setAlbum(b);
        pc.getMainCache().addGraphToCache(graph3, pc);
        markAllNodeAsDirty();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graph3.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(1, fs.size());
        flushManager.clearFlushStack();
    }

    @Test
    public void test_M_M_1_1()
    {
        FlushManager flushManager = new FlushManager();
        PhotographerUni_M_M_1_1 a1 = new PhotographerUni_M_M_1_1();
        a1.setPhotographerId(1);
        PhotographerUni_M_M_1_1 a2 = new PhotographerUni_M_M_1_1();
        a2.setPhotographerId(2);

        AlbumUni_M_M_1_1 b1 = new AlbumUni_M_M_1_1();
        b1.setAlbumId("b1");
        AlbumUni_M_M_1_1 b2 = new AlbumUni_M_M_1_1();
        b2.setAlbumId("b2");
        AlbumUni_M_M_1_1 b3 = new AlbumUni_M_M_1_1();
        b3.setAlbumId("b3");

        PhotoUni_M_M_1_1 c1 = new PhotoUni_M_M_1_1();
        c1.setPhotoId("c1");
        PhotoUni_M_M_1_1 c2 = new PhotoUni_M_M_1_1();
        c2.setPhotoId("c2");
        PhotoUni_M_M_1_1 c3 = new PhotoUni_M_M_1_1();
        c3.setPhotoId("c3");

        b1.setPhoto(c1);
        b2.setPhoto(c2);
        b3.setPhoto(c3);
        a1.addAlbum(b1);
        a1.addAlbum(b2);

        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1, null);

        pc.getMainCache().addGraphToCache(graph1, pc);

        markAllNodeAsDirty();

        flushManager.buildFlushStack(graph1.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(5, fs.size());
        flushManager.clearFlushStack();

        a2.addAlbum(b2);
        a2.addAlbum(b3);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2, null);
        pc.getMainCache().addGraphToCache(graph2, pc);
        markAllNodeAsDirty();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graph2.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(3, fs.size());
    }

    @Test
    public void test_M_M_M_M()
    {
        FlushManager flushManager = new FlushManager();

        PhotographerUni_M_M_M_M a1 = new PhotographerUni_M_M_M_M();
        a1.setPhotographerId(1);
        PhotographerUni_M_M_M_M a2 = new PhotographerUni_M_M_M_M();
        a2.setPhotographerId(2);

        AlbumUni_M_M_M_M b1 = new AlbumUni_M_M_M_M();
        b1.setAlbumId("b1");
        AlbumUni_M_M_M_M b2 = new AlbumUni_M_M_M_M();
        b2.setAlbumId("b2");
        AlbumUni_M_M_M_M b3 = new AlbumUni_M_M_M_M();
        b3.setAlbumId("b3");

        PhotoUni_M_M_M_M c1 = new PhotoUni_M_M_M_M();
        c1.setPhotoId("c1");
        PhotoUni_M_M_M_M c2 = new PhotoUni_M_M_M_M();
        c2.setPhotoId("c2");
        PhotoUni_M_M_M_M c3 = new PhotoUni_M_M_M_M();
        c3.setPhotoId("c3");
        PhotoUni_M_M_M_M c4 = new PhotoUni_M_M_M_M();
        c4.setPhotoId("c4");

        b1.addPhoto(c1);
        b1.addPhoto(c2);
        b2.addPhoto(c2);
        b2.addPhoto(c3);
        b3.addPhoto(c3);
        b3.addPhoto(c4);
        a1.addAlbum(b1);
        a1.addAlbum(b2);

        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1, null);

        pc.getMainCache().addGraphToCache(graph1, pc);

        markAllNodeAsDirty();
        flushManager.buildFlushStack(graph1.getHeadNode(), EventType.INSERT);
        Deque<Node> fs = flushManager.getFlushStack();
        Assert.assertEquals(6, fs.size());
        flushManager.clearFlushStack();

        a2.addAlbum(b2);
        a2.addAlbum(b3);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2, null);
        pc.getMainCache().addGraphToCache(graph2, pc);

        markAllNodeAsDirty();
        flushManager = new FlushManager();
        flushManager.buildFlushStack(graph2.getHeadNode(), EventType.INSERT);
        fs = flushManager.getFlushStack();
        Assert.assertEquals(3, fs.size());
    }

    /**
     * 
     */
    private void markAllNodeAsDirty()
    {
        if (pc.getMainCache() != null)
        {
            Set<Node> headNodes = pc.getMainCache().getHeadNodes();
            if (headNodes != null)
            {

                for (Node hn : headNodes)
                {
                    if (hn != null)
                        hn.setDirty(true);

                }
            }

            Collection<Node> allNodes = pc.getMainCache().getAllNodes();
            if (allNodes != null)
            {
                for (Node node : allNodes)
                {
                    if (node != null)
                        node.setDirty(true);
                }
            }
        }
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
        
        Map<String, Object> props = new HashMap<String, Object>();

        props.put(Constants.PERSISTENCE_UNIT_NAME, _persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, CoreTestClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaMetaDataTest");

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
//        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(_persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(_persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);


        clazzToPu.put(Store.class.getName(), pus);
        clazzToPu.put(BillingCounter.class.getName(), pus);
        clazzToPu.put(PhotographerUni_1_1_1_1.class.getName(), pus);
        clazzToPu.put(AlbumUni_1_1_1_1.class.getName(), pus);
        clazzToPu.put(PhotoUni_1_1_1_1.class.getName(), pus);
        clazzToPu.put(PhotographerUni_1_1_1_M.class.getName(), pus);
        clazzToPu.put(AlbumUni_1_1_1_M.class.getName(), pus);
        clazzToPu.put(PhotoUni_1_1_1_M.class.getName(), pus);
        clazzToPu.put(PhotographerUni_1_1_M_1.class.getName(), pus);
        clazzToPu.put(AlbumUni_1_1_M_1.class.getName(), pus);
        clazzToPu.put(PhotoUni_1_1_M_1.class.getName(), pus);
        clazzToPu.put(PhotographerUni_1_M_1_M.class.getName(), pus);
        clazzToPu.put(AlbumUni_1_M_1_M.class.getName(), pus);
        clazzToPu.put(PhotoUni_1_M_1_M.class.getName(), pus);
        clazzToPu.put(PhotographerUni_1_M_M_M.class.getName(), pus);
        clazzToPu.put(AlbumUni_1_M_M_M.class.getName(), pus);
        clazzToPu.put(PhotoUni_1_M_M_M.class.getName(), pus);
        clazzToPu.put(PhotographerUni_M_1_1_M.class.getName(), pus);
        clazzToPu.put(AlbumUni_M_1_1_M.class.getName(), pus);
        clazzToPu.put(PhotoUni_M_1_1_M.class.getName(), pus);
        clazzToPu.put(PhotographerUni_M_M_1_1.class.getName(), pus);
        clazzToPu.put(AlbumUni_M_M_1_1.class.getName(), pus);
        clazzToPu.put(PhotoUni_M_M_1_1.class.getName(), pus);
        clazzToPu.put(PhotographerUni_M_M_M_M.class.getName(), pus);
        clazzToPu.put(AlbumUni_M_M_M_M.class.getName(), pus);
        clazzToPu.put(PhotoUni_M_M_M_M.class.getName(), pus);
        clazzToPu.put(PhotographerBi_1_1_1_1.class.getName(), pus);
        clazzToPu.put(AlbumBi_1_1_1_1.class.getName(), pus);
        clazzToPu.put(PhotoBi_1_1_1_1.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);
        
        KunderaMetadata.INSTANCE.setApplicationMetadata(appMetadata);

        MetamodelImpl metaModel = new MetamodelImpl();
        
        MetadataBuilder metadataBuilder = new MetadataBuilder(_persistenceUnit, CoreTestClient.class.getSimpleName(), null);

        metaModel.addEntityMetadata(Store.class, metadataBuilder.buildEntityMetadata(Store.class));
        metaModel.addEntityMetadata(BillingCounter.class, metadataBuilder.buildEntityMetadata(BillingCounter.class));
        metaModel.addEntityMetadata(PhotographerUni_1_1_1_1.class, metadataBuilder.buildEntityMetadata(PhotographerUni_1_1_1_1.class));
        metaModel.addEntityMetadata(AlbumUni_1_1_1_1.class, metadataBuilder.buildEntityMetadata(AlbumUni_1_1_1_1.class));
        metaModel.addEntityMetadata(PhotoUni_1_1_1_1.class, metadataBuilder.buildEntityMetadata(PhotoUni_1_1_1_1.class));
        metaModel.addEntityMetadata(PhotographerUni_1_1_1_M.class, metadataBuilder.buildEntityMetadata(PhotographerUni_1_1_1_M.class));
        metaModel.addEntityMetadata(AlbumUni_1_1_1_M.class, metadataBuilder.buildEntityMetadata(AlbumUni_1_1_1_M.class));
        metaModel.addEntityMetadata(PhotoUni_1_1_1_M.class, metadataBuilder.buildEntityMetadata(PhotoUni_1_1_1_M.class));
        metaModel.addEntityMetadata(PhotographerUni_1_1_M_1.class, metadataBuilder.buildEntityMetadata(PhotographerUni_1_1_M_1.class));
        metaModel.addEntityMetadata(AlbumUni_1_1_M_1.class, metadataBuilder.buildEntityMetadata(AlbumUni_1_1_M_1.class));
        metaModel.addEntityMetadata(PhotoUni_1_1_M_1.class, metadataBuilder.buildEntityMetadata(PhotoUni_1_1_M_1.class));
        metaModel.addEntityMetadata(PhotographerUni_1_M_1_M.class, metadataBuilder.buildEntityMetadata(PhotographerUni_1_M_1_M.class));
        metaModel.addEntityMetadata(AlbumUni_1_M_1_M.class, metadataBuilder.buildEntityMetadata(AlbumUni_1_M_1_M.class));
        metaModel.addEntityMetadata(PhotoUni_1_M_1_M.class, metadataBuilder.buildEntityMetadata(PhotoUni_1_M_1_M.class));
        metaModel.addEntityMetadata(PhotographerUni_1_M_M_M.class, metadataBuilder.buildEntityMetadata(PhotographerUni_1_M_M_M.class));
        metaModel.addEntityMetadata(AlbumUni_1_M_M_M.class, metadataBuilder.buildEntityMetadata(AlbumUni_1_M_M_M.class));
        metaModel.addEntityMetadata(PhotoUni_1_M_M_M.class, metadataBuilder.buildEntityMetadata(PhotoUni_1_M_M_M.class));
        metaModel.addEntityMetadata(PhotographerUni_M_1_1_M.class, metadataBuilder.buildEntityMetadata(PhotographerUni_M_1_1_M.class));
        metaModel.addEntityMetadata(AlbumUni_M_1_1_M.class, metadataBuilder.buildEntityMetadata(AlbumUni_M_1_1_M.class));
        metaModel.addEntityMetadata(PhotoUni_M_1_1_M.class, metadataBuilder.buildEntityMetadata(PhotoUni_M_1_1_M.class));
        metaModel.addEntityMetadata(PhotographerUni_M_M_1_1.class, metadataBuilder.buildEntityMetadata(PhotographerUni_M_M_1_1.class));
        metaModel.addEntityMetadata(AlbumUni_M_M_1_1.class, metadataBuilder.buildEntityMetadata(AlbumUni_M_M_1_1.class));
        metaModel.addEntityMetadata(PhotoUni_M_M_1_1.class, metadataBuilder.buildEntityMetadata(PhotoUni_M_M_1_1.class));
        metaModel.addEntityMetadata(PhotographerUni_M_M_M_M.class, metadataBuilder.buildEntityMetadata(PhotographerUni_M_M_M_M.class));
        metaModel.addEntityMetadata(AlbumUni_M_M_M_M.class, metadataBuilder.buildEntityMetadata(AlbumUni_M_M_M_M.class));
        metaModel.addEntityMetadata(PhotoUni_M_M_M_M.class, metadataBuilder.buildEntityMetadata(PhotoUni_M_M_M_M.class));

        metaModel.addEntityMetadata(PhotographerBi_1_1_1_1.class, metadataBuilder.buildEntityMetadata(PhotographerBi_1_1_1_1.class));
        metaModel.addEntityMetadata(AlbumBi_1_1_1_1.class, metadataBuilder.buildEntityMetadata(AlbumBi_1_1_1_1.class));
        metaModel.addEntityMetadata(PhotoBi_1_1_1_1.class, metadataBuilder.buildEntityMetadata(PhotoBi_1_1_1_1.class));

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);
        return null;
    }
}
