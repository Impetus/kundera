/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.lifecycle.states;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import javax.persistence.CascadeType;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContextType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.StoreBuilder;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 */
public class ManagedStateTest
{
    private PersistenceCache pc;

    private ManagedState state;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        state = new ManagedState();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        pc = null;
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#initialize(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testInitialize()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.initialize(storeNode);
        Assert.assertNotNull(pc);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handlePersist(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandlePersist()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handlePersist(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleRemove(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRemove()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REMOVE);
        state.handleRemove(storeNode);

        Assert.assertEquals(RemovedState.class, storeNode.getCurrentNodeState().getClass());
        Assert.assertTrue(storeNode.isDirty());

        // for (Node childNode : storeNode.getChildren().values())
        // {
        // Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
        // Assert.assertEquals(RemovedState.class,
        // childNode.getCurrentNodeState().getClass());
        // Assert.assertTrue(childNode.isDirty());
        // }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleRefresh(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRefresh()
    {
        try
        {
            Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
            state.handleRefresh(storeNode);
            Assert.fail("Exception should be thrown because client is not available");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleMerge(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IllegalArgumentException
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    @Test
    public void testHandleMerge() throws IllegalArgumentException, InstantiationException, IllegalAccessException,
            InvocationTargetException, SecurityException, NoSuchMethodException
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.MERGE);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("kunderatest");
        Constructor constructor = PersistenceDelegator.class.getDeclaredConstructor(KunderaMetadata.class,
                PersistenceCache.class);
        constructor.setAccessible(true);
        PersistenceDelegator pd = (PersistenceDelegator) constructor.newInstance(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), new PersistenceCache());

        storeNode.setPersistenceDelegator(pd);
        
        state.handleMerge(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());
        Assert.assertTrue(storeNode.isUpdate());
        Assert.assertNotNull(storeNode.getPersistenceCache().getMainCache()
                .getNodeFromCache(storeNode.getNodeId(), null));

        // for (Node childNode : storeNode.getChildren().values())
        // {
        // Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
        // Assert.assertEquals(ManagedState.class,
        // childNode.getCurrentNodeState().getClass());
        // Assert.assertTrue(childNode.isUpdate());
        // Assert.assertNotNull(childNode.getPersistenceCache().getMainCache().getNodeFromCache(childNode.getNodeId()));
        // }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleDetach(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleDetach()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.DETACH);
        state.handleDetach(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(DetachedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleClear(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleClear()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.DETACH);
        state.handleClear(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(DetachedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleClose(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleClose()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.DETACH);
        state.handleClose(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(DetachedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleLock(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleLock()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleLock(storeNode);
        Assert.assertTrue(storeNode.getCurrentNodeState().getClass().equals(ManagedState.class));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleCommit(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleCommit()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleCommit(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleRollback(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    // @Test
    public void testHandleRollback()
    {
        // Extended
        pc.setPersistenceContextType(PersistenceContextType.EXTENDED);
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleDetach(storeNode);

        Assert.assertEquals(TransientState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }

        // Transactional
        pc.setPersistenceContextType(PersistenceContextType.TRANSACTION);
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleDetach(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleFind(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFind()
    {
        try
        {
            Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
            state.handleFind(storeNode);
            Assert.fail("Exception should be thrown because client is not available");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleGetReference(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleGetReference()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleGetReference(storeNode);
        Assert.assertTrue(storeNode.getCurrentNodeState().getClass().equals(ManagedState.class));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleContains(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleContains()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleContains(storeNode);
        Assert.assertTrue(storeNode.getCurrentNodeState().getClass().equals(ManagedState.class));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.ManagedState#handleFlush(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFlush()
    {
        try
        {
            Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
            state.handleFlush(storeNode);
            Assert.fail("Exception should be thrown because client is not available");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }

}
