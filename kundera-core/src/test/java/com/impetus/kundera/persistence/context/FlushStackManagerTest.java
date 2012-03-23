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
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_1_1_1_1;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_1_1_1_M;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_1_1_M_1;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_1_M_M_M;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_M_1_1_M;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_M_M_1_1;
import com.impetus.kundera.persistence.context.entities.album.AlbumUni_M_M_M_M;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_1_1_1_1;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_1_1_1_M;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_1_1_M_1;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_1_M_M_M;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_M_1_1_M;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_M_M_1_1;
import com.impetus.kundera.persistence.context.entities.photo.PhotoUni_M_M_M_M;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_1_1_1_1;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_1_1_1_M;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_1_1_M_1;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_1_M_1_M;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_1_M_M_M;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_M_1_1_M;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_M_M_1_1;
import com.impetus.kundera.persistence.context.entities.photographer.PhotographerUni_M_M_M_M;

/**
 * Test case for {@link FlushStackManager} 
 * @author amresh.singh
 */
public class FlushStackManagerTest
{
    PersistenceCache pc;
    FlushStackManager flushStackManager;    
    ObjectGraphBuilder graphBuilder;    
    
    Configurator configurator = new Configurator("kunderatest");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        flushStackManager = new FlushStackManager();
        graphBuilder = new ObjectGraphBuilder();
        
        configurator.configure();   
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
    
    @Test
    public void testFlashStockForStore() {
        Store store = new Store(1, "Food Bazaar, Noida");
        store.addCounter(new BillingCounter(1, "A"));
        store.addCounter(new BillingCounter(2, "B"));
        store.addCounter(new BillingCounter(3, "C"));
        
        ObjectGraph graph = graphBuilder.getObjectGraph(store);
        
        pc.getMainCache().addGraphToCache(graph);
        
        Assert.assertNotNull(pc.getMainCache());
        Assert.assertEquals(1, pc.getMainCache().getHeadNodes().size());
        Assert.assertNull(pc.getMainCache().getHeadNodes().get(0).getParents());
        Assert.assertEquals(3, pc.getMainCache().getHeadNodes().get(0).getChildren().size());
        
        Assert.assertEquals(4, pc.getMainCache().getNodeMappings().size());
        
        
        flushStackManager.buildFlushStack(pc);        
        
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(4, fs.size());      
    } 
    
    @Test
    public void test_1_1_1_1() {
        PhotographerUni_1_1_1_1 a = new PhotographerUni_1_1_1_1(); a.setPhotographerId(1);
        AlbumUni_1_1_1_1 b = new AlbumUni_1_1_1_1(); b.setAlbumId("b1");
        PhotoUni_1_1_1_1 c = new PhotoUni_1_1_1_1(); c.setPhotoId("c1");       
        a.setAlbum(b); b.setPhoto(c);
        
        ObjectGraph graph = graphBuilder.getObjectGraph(a);
        pc.getMainCache().addGraphToCache(graph);
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(3, fs.size());
    }
    
    @Test
    public void test_1_1_1_M() {
        PhotographerUni_1_1_1_M a = new PhotographerUni_1_1_1_M(); a.setPhotographerId(1);
        AlbumUni_1_1_1_M b = new AlbumUni_1_1_1_M(); b.setAlbumId("b1");
        PhotoUni_1_1_1_M c1 = new PhotoUni_1_1_1_M(); c1.setPhotoId("c1");       
        PhotoUni_1_1_1_M c2 = new PhotoUni_1_1_1_M(); c2.setPhotoId("c2");
        PhotoUni_1_1_1_M c3 = new PhotoUni_1_1_1_M(); c3.setPhotoId("c3");
        a.setAlbum(b); b.addPhoto(c1);b.addPhoto(c2);b.addPhoto(c3);
        
        ObjectGraph graph = graphBuilder.getObjectGraph(a);
        pc.getMainCache().addGraphToCache(graph);
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(5, fs.size());
    }
    
    @Test
    public void test_1_1_M_1() {
        PhotographerUni_1_1_M_1 a = new PhotographerUni_1_1_M_1(); a.setPhotographerId(1);
        AlbumUni_1_1_M_1 b1 = new AlbumUni_1_1_M_1(); b1.setAlbumId("b1");
        AlbumUni_1_1_M_1 b2 = new AlbumUni_1_1_M_1(); b2.setAlbumId("b2");
        AlbumUni_1_1_M_1 b3 = new AlbumUni_1_1_M_1(); b3.setAlbumId("b3");
        
        
        PhotoUni_1_1_M_1 c = new PhotoUni_1_1_M_1(); c.setPhotoId("c");       
        a.setAlbum(b1); b1.setPhoto(c);b2.setPhoto(c);b3.setPhoto(c);
        
        ObjectGraph graph = graphBuilder.getObjectGraph(a);        
        ObjectGraph graphb2 = graphBuilder.getObjectGraph(b2);
        ObjectGraph graphb3 = graphBuilder.getObjectGraph(b3);
        
        pc.getMainCache().addGraphToCache(graph);
        pc.getMainCache().addGraphToCache(graphb2);
        pc.getMainCache().addGraphToCache(graphb3);
        
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(5, fs.size());
    }
    
    @Test
    public void test_1_M_1_M() {
        PhotographerUni_1_M_1_M a = new PhotographerUni_1_M_1_M(); a.setPhotographerId(1);
        AlbumUni_1_M_1_M b1 = new AlbumUni_1_M_1_M(); b1.setAlbumId("b1");
        AlbumUni_1_M_1_M b2 = new AlbumUni_1_M_1_M(); b2.setAlbumId("b2");
        
        PhotoUni_1_M_1_M c1 = new PhotoUni_1_M_1_M(); c1.setPhotoId("c1");
        PhotoUni_1_M_1_M c2 = new PhotoUni_1_M_1_M(); c2.setPhotoId("c2");
        PhotoUni_1_M_1_M c3 = new PhotoUni_1_M_1_M(); c3.setPhotoId("c3");
        PhotoUni_1_M_1_M c4 = new PhotoUni_1_M_1_M(); c4.setPhotoId("c4");

        
        b1.addPhoto(c1);b1.addPhoto(c2);b2.addPhoto(c3);b2.addPhoto(c4);
        a.addAlbum(b1);a.addAlbum(b2);
        
        ObjectGraph graph = graphBuilder.getObjectGraph(a);
        pc.getMainCache().addGraphToCache(graph);
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(7, fs.size());
    }
    
    @Test
    public void test_1_M_M_M() {
        PhotographerUni_1_M_M_M a = new PhotographerUni_1_M_M_M(); a.setPhotographerId(1);
        AlbumUni_1_M_M_M b1 = new AlbumUni_1_M_M_M(); b1.setAlbumId("b1");
        AlbumUni_1_M_M_M b2 = new AlbumUni_1_M_M_M(); b2.setAlbumId("b2");
        
        PhotoUni_1_M_M_M c1 = new PhotoUni_1_M_M_M(); c1.setPhotoId("c1");
        PhotoUni_1_M_M_M c2 = new PhotoUni_1_M_M_M(); c2.setPhotoId("c2");
        PhotoUni_1_M_M_M c3 = new PhotoUni_1_M_M_M(); c3.setPhotoId("c3");
        

        
        b1.addPhoto(c1);b1.addPhoto(c2);b2.addPhoto(c2);b2.addPhoto(c3);
        a.addAlbum(b1);a.addAlbum(b2);
        
        ObjectGraph graph = graphBuilder.getObjectGraph(a);
        pc.getMainCache().addGraphToCache(graph);
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(6, fs.size());
    }
    
    @Test
    public void test_M_1_1_M() {
        PhotographerUni_M_1_1_M a1 = new PhotographerUni_M_1_1_M(); a1.setPhotographerId(1);
        PhotographerUni_M_1_1_M a2 = new PhotographerUni_M_1_1_M(); a2.setPhotographerId(2);
        PhotographerUni_M_1_1_M a3 = new PhotographerUni_M_1_1_M(); a3.setPhotographerId(3);
        
        AlbumUni_M_1_1_M b = new AlbumUni_M_1_1_M(); b.setAlbumId("b");
        
        PhotoUni_M_1_1_M c1 = new PhotoUni_M_1_1_M(); c1.setPhotoId("c1");
        PhotoUni_M_1_1_M c2 = new PhotoUni_M_1_1_M(); c2.setPhotoId("c2");
        PhotoUni_M_1_1_M c3 = new PhotoUni_M_1_1_M(); c3.setPhotoId("c3");
        
        b.addPhoto(c1);b.addPhoto(c2);b.addPhoto(c3);
        a1.setAlbum(b);a2.setAlbum(b);a3.setAlbum(b);
        
        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2);
        ObjectGraph graph3 = graphBuilder.getObjectGraph(a3);
        
        pc.getMainCache().addGraphToCache(graph1);
        pc.getMainCache().addGraphToCache(graph2);
        pc.getMainCache().addGraphToCache(graph3);
        
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(7, fs.size());
    }
    
    @Test
    public void test_M_M_1_1() {
        PhotographerUni_M_M_1_1 a1 = new PhotographerUni_M_M_1_1(); a1.setPhotographerId(1);
        PhotographerUni_M_M_1_1 a2 = new PhotographerUni_M_M_1_1(); a2.setPhotographerId(2);
        
        
        AlbumUni_M_M_1_1 b1 = new AlbumUni_M_M_1_1(); b1.setAlbumId("b1");
        AlbumUni_M_M_1_1 b2 = new AlbumUni_M_M_1_1(); b2.setAlbumId("b2");
        AlbumUni_M_M_1_1 b3 = new AlbumUni_M_M_1_1(); b3.setAlbumId("b3");
        
        PhotoUni_M_M_1_1 c1 = new PhotoUni_M_M_1_1(); c1.setPhotoId("c1");
        PhotoUni_M_M_1_1 c2 = new PhotoUni_M_M_1_1(); c2.setPhotoId("c2");
        PhotoUni_M_M_1_1 c3 = new PhotoUni_M_M_1_1(); c3.setPhotoId("c3");
        
        b1.setPhoto(c1);b2.setPhoto(c2);b3.setPhoto(c3);
        a1.addAlbum(b1);a1.addAlbum(b2);
        a2.addAlbum(b2);a2.addAlbum(b3);
        
        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2);        
        
        pc.getMainCache().addGraphToCache(graph1);
        pc.getMainCache().addGraphToCache(graph2);        
        
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(8, fs.size());
    }
    
    @Test
    public void test_M_M_M_M() {
        PhotographerUni_M_M_M_M a1 = new PhotographerUni_M_M_M_M(); a1.setPhotographerId(1);
        PhotographerUni_M_M_M_M a2 = new PhotographerUni_M_M_M_M(); a2.setPhotographerId(2);
        
        
        AlbumUni_M_M_M_M b1 = new AlbumUni_M_M_M_M(); b1.setAlbumId("b1");
        AlbumUni_M_M_M_M b2 = new AlbumUni_M_M_M_M(); b2.setAlbumId("b2");
        AlbumUni_M_M_M_M b3 = new AlbumUni_M_M_M_M(); b3.setAlbumId("b3");
        
        PhotoUni_M_M_M_M c1 = new PhotoUni_M_M_M_M(); c1.setPhotoId("c1");
        PhotoUni_M_M_M_M c2 = new PhotoUni_M_M_M_M(); c2.setPhotoId("c2");
        PhotoUni_M_M_M_M c3 = new PhotoUni_M_M_M_M(); c3.setPhotoId("c3");
        PhotoUni_M_M_M_M c4 = new PhotoUni_M_M_M_M(); c4.setPhotoId("c4");
        
        b1.addPhoto(c1);b1.addPhoto(c2);b2.addPhoto(c2);b2.addPhoto(c3);b3.addPhoto(c3);b3.addPhoto(c4);        
        a1.addAlbum(b1);a1.addAlbum(b2);a2.addAlbum(b2);a2.addAlbum(b3);
        
        ObjectGraph graph1 = graphBuilder.getObjectGraph(a1);
        ObjectGraph graph2 = graphBuilder.getObjectGraph(a2);        
        
        pc.getMainCache().addGraphToCache(graph1);
        pc.getMainCache().addGraphToCache(graph2);        
        
        flushStackManager.buildFlushStack(pc);  
        FlushStack fs = pc.getFlushStack();
        System.out.println(fs);
        Assert.assertEquals(9, fs.size());
    }
    
   

   
}
