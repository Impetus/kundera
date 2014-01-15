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

import javax.persistence.CascadeType;
import javax.persistence.PersistenceContextType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.StoreBuilder;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 * 
 */
public class RemovedStateTest
{

    PersistenceCache pc;

    RemovedState state;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        state = new RemovedState();
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#initialize(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handlePersist(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandlePersist()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handlePersist(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());

//        for (Node childNode : storeNode.getChildren().values())
//        {
//            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
//        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleRemove(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRemove()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REMOVE);
        state.handleRemove(storeNode);

        Assert.assertEquals(RemovedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(RemovedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleRefresh(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRefresh()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
        try
        {
            state.handleRefresh(storeNode);
            Assert.fail("Refresh operation in Removed state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleMerge(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleMerge()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.MERGE);
        try
        {
            state.handleMerge(storeNode);
            Assert.fail("Merge operation in Removed state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleDetach(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleClose(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleLock(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleCommit(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleCommit()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleCommit(storeNode);

        Assert.assertEquals(TransientState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(RemovedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleRollback(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRollback()
    {
        // Extended
        pc.setPersistenceContextType(PersistenceContextType.EXTENDED);
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleRollback(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(RemovedState.class, childNode.getCurrentNodeState().getClass());
        }

        // Transactional
        pc.setPersistenceContextType(PersistenceContextType.TRANSACTION);
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleRollback(storeNode);

        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
            Assert.assertEquals(RemovedState.class, childNode.getCurrentNodeState().getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleFind(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleGetReference(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleContains(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleClear(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.RemovedState#handleFlush(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFlush()
    {
        try
        {
            Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
            state.handleFlush(storeNode);
            Assert.fail("Exception should be thrown because client is not available");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }

}
