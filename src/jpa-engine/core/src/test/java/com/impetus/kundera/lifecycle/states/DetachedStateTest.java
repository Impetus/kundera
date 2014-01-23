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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.StoreBuilder;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 * 
 */
public class DetachedStateTest
{
    PersistenceCache pc;

    DetachedState state;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
        state = new DetachedState();
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#initialize(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handlePersist(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandlePersist()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        try
        {
            state.handlePersist(storeNode);
            Assert.fail("Persist operation in Detached state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleRemove(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRemove()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REMOVE);
        try
        {
            state.handleRemove(storeNode);
            Assert.fail("Remove operation in Detached state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleRefresh(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleRefresh()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
        try
        {
            state.handleRefresh(storeNode);
            Assert.fail("refresh operation in Detached state should have thrown exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleMerge(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleMerge()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.MERGE);
        state.handleMerge(storeNode);

        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());
        Assert.assertTrue(storeNode.isUpdate());

//        for (Node childNode : storeNode.getChildren().values())
//        {
//            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
//            Assert.assertTrue(childNode.isUpdate());
//        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleDetach(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleDetach()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleDetach(storeNode);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleClose(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleLock(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleCommit(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleRollback(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleFind(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleGetReference(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleContains(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleClear(com.impetus.kundera.lifecycle.NodeStateContext)}
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
     * {@link com.impetus.kundera.lifecycle.states.DetachedState#handleFlush(com.impetus.kundera.lifecycle.NodeStateContext)}
     * .
     */
    @Test
    public void testHandleFlush()
    {
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.handleFlush(storeNode);
    }

}
