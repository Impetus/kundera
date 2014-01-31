/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.StoreBuilder;
import com.impetus.kundera.lifecycle.NodeStateContext;
import com.impetus.kundera.lifecycle.states.NodeState.OPERATION;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 */
public class NodeStateTest
{

    PersistenceCache pc;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pc = new PersistenceCache();
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
     * {@link com.impetus.kundera.lifecycle.states.NodeState#moveNodeToNextState(com.impetus.kundera.lifecycle.NodeStateContext, com.impetus.kundera.lifecycle.states.NodeState)}
     * .
     */
    @Test
    public void testMoveNodeToNextState()
    {
        NodeState nodeState = new TransientState();
        NodeStateContext node = new Node("1", PersonnelDTO.class, nodeState, pc, "1", null);
        nodeState.moveNodeToNextState(node, new ManagedState());
        Assert.assertEquals(ManagedState.class, node.getCurrentNodeState().getClass());
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.lifecycle.states.NodeState#recursivelyPerformOperation(com.impetus.kundera.lifecycle.NodeStateContext, com.impetus.kundera.lifecycle.states.NodeState.OPERATION)}
     * .
     */
    @Test
    public void testRecursivelyPerformOperation()
    {
        // Persist operation
        NodeState state = new TransientState();
        Node storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.PERSIST);
        state.recursivelyPerformOperation(storeNode, OPERATION.PERSIST);
        Assert.assertEquals(TransientState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }

        // Merge operation
        state = new DetachedState();
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.MERGE);
        state.recursivelyPerformOperation(storeNode, OPERATION.MERGE);
        Assert.assertEquals(DetachedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
        }

        // Remove Operation
        state = new ManagedState();
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REMOVE);
        state.recursivelyPerformOperation(storeNode, OPERATION.REMOVE);
        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(RemovedState.class, childNode.getCurrentNodeState().getClass());
        }

        // Refresh Operation
        state = new DetachedState();
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.REFRESH);
        try
        {
            state.recursivelyPerformOperation(storeNode, OPERATION.REFRESH);
            Assert.fail("Refresh operation in Detached state should have thrown an exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
        }

        // Remove Operation
        state = new ManagedState();
        storeNode = StoreBuilder.buildStoreNode(pc, state, CascadeType.DETACH);
        state.recursivelyPerformOperation(storeNode, OPERATION.DETACH);
        Assert.assertEquals(ManagedState.class, storeNode.getCurrentNodeState().getClass());

        for (Node childNode : storeNode.getChildren().values())
        {
            Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
//            Assert.assertEquals(DetachedState.class, childNode.getCurrentNodeState().getClass());
        }

    }

}
