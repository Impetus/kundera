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
import com.impetus.kundera.utils.DeepEquals;

/**
 * @author amresh.singh
 * 
 */
public class TransientStateTest
{
    PersistenceCache pc;

    TransientState state;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        state = new TransientState();
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
     * {@link com.impetus.kundera.lifecycle.states.TransientState#initialize(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handlePersist(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IllegalArgumentException
     */
    @Test
    public void testHandlePersist() throws SecurityException, NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException, InvocationTargetException
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("kunderatest");
        Constructor constructor = PersistenceDelegator.class.getDeclaredConstructor(KunderaMetadata.class,
                PersistenceCache.class);
        constructor.setAccessible(true);
        PersistenceDelegator pd = (PersistenceDelegator) constructor.newInstance(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), new PersistenceCache());

        storeNode.setPersistenceDelegator(pd);

        state.handlePersist(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());
        Assert.assertTrue(storeNode.isDirty());
        Assert.assertNotNull(storeNode.getPersistenceCache().getMainCache()
                .getNodeFromCache(storeNode.getNodeId(), null));

        // for (Node childNode : storeNode.getChildren().values())
        // {
        // Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
        // Assert.assertEquals(ManagedState.class,
        // childNode.getCurrentNodeState().getClass());
        // Assert.assertTrue(childNode.isDirty());
        // Assert.assertNotNull(childNode.getPersistenceCache().getMainCache().getNodeFromCache(childNode.getNodeId()));
        // }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleRemove(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRemove()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REMOVE);
        state.handleRemove(storeNode);

        Assert.assertEquals(TransientState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(TransientState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleRefresh(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRefresh()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
        try
        {
            state.handleRefresh(storeNode);
            Assert.fail("Refresh operation in Transient state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleMerge(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IllegalArgumentException
     */
    @Test
    public void testHandleMerge() throws SecurityException, NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException, InvocationTargetException
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.MERGE);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("kunderatest");
        Constructor constructor = PersistenceDelegator.class.getDeclaredConstructor(KunderaMetadata.class,
                PersistenceCache.class);
        constructor.setAccessible(true);
        PersistenceDelegator pd = (PersistenceDelegator) constructor.newInstance(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), new PersistenceCache());

        storeNode.setPersistenceDelegator(pd);

        Object data1 = storeNode.getData();
        state.handleMerge(storeNode);
        Object data2 = storeNode.getData();
        Assert.assertTrue(DeepEquals.deepEquals(data1, data2));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleDetach(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleDetach()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.DETACH);
        state.handleDetach(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleClose(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleClose()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleClose(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleLock(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleLock()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleLock(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleCommit(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleCommit()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleCommit(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleRollback(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRollback()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleRollback(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleFind(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFind()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleFind(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleGetReference(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleGetReference()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleGetReference(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleContains(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleContains()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleContains(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleClear(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleClear()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleClear(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.TransientState#handleFlush(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFlush()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleFlush(storeNode);
    }

}
