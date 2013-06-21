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

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.graph.StoreBuilder;
import com.impetus.kundera.lifecycle.NodeStateContext;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.lifecycle.states.TransientState;
import com.impetus.kundera.lifecycle.states.NodeState.OPERATION;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 */
public class NodeStateTest {

	PersistenceCache pc;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		pc = new PersistenceCache();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.impetus.kundera.lifecycle.states.NodeState#moveNodeToNextState(com.impetus.kundera.lifecycle.NodeStateContext, com.impetus.kundera.lifecycle.states.NodeState)}.
	 */
	@Test
	public void testMoveNodeToNextState() {	
		NodeState nodeState = new TransientState();
		NodeStateContext node = new Node("1", PersonnelDTO.class, nodeState, pc, "1");
		nodeState.moveNodeToNextState(node, new ManagedState());
		Assert.assertEquals(ManagedState.class, node.getCurrentNodeState().getClass());
	}

	/**
	 * Test method for {@link com.impetus.kundera.lifecycle.states.NodeState#recursivelyPerformOperation(com.impetus.kundera.lifecycle.NodeStateContext, com.impetus.kundera.lifecycle.states.NodeState.OPERATION)}.
	 */
	@Test
	public void testRecursivelyPerformOperation() {
		Node storeNode = StoreBuilder.buildStoreNode(pc);
		NodeState state = new TransientState();
		state.recursivelyPerformOperation(storeNode, OPERATION.PERSIST);
		
		Assert.assertEquals(TransientState.class, storeNode.getCurrentNodeState().getClass());
		
		for(Node childNode : storeNode.getChildren().values())
		{
			Assert.assertEquals(BillingCounter.class, childNode.getDataClass());
			Assert.assertEquals(ManagedState.class, childNode.getCurrentNodeState().getClass());
		}
		
	}

}
