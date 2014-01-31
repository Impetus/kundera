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
package com.impetus.kundera.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * Test case for {@link ObjectGraph}
 * 
 * @author amresh.singh
 */
public class ObjectGraphTest
{
    ObjectGraph objectGraph;

    PersistenceCache pc;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        objectGraph = new ObjectGraph();
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
     * Tests one parent and three child nodes
     */
    @Test
    public void testOneParentAndThreeChildNodes()
    {
        Store store = new Store();
        BillingCounter b1 = new BillingCounter();
        BillingCounter b2 = new BillingCounter();
        BillingCounter b3 = new BillingCounter();

        String storeId = ObjectGraphUtils.getNodeId("1", store.getClass());
        String b1Id = ObjectGraphUtils.getNodeId("A1", b1.getClass());
        String b2Id = ObjectGraphUtils.getNodeId("A2", b2.getClass());
        String b3Id = ObjectGraphUtils.getNodeId("A3", b3.getClass());

        Node headNode = new Node(storeId, store, null, pc, "1", null);

        Node child1 = new Node(b1Id, b1, null, pc, "A1", null);
        Node child2 = new Node(b2Id, b2, null, pc, "A2", null);
        Node child3 = new Node(b3Id, b3, null, pc, "A3", null);

        NodeLink linkB1 = new NodeLink(storeId, b1Id);
        NodeLink linkB2 = new NodeLink(storeId, b2Id);
        NodeLink linkB3 = new NodeLink(storeId, b3Id);

        linkB1.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");
        linkB2.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");
        linkB3.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");

        headNode.addChildNode(linkB1, child1);
        headNode.addChildNode(linkB2, child2);
        headNode.addChildNode(linkB3, child3);

        child1.addParentNode(linkB1, headNode);
        child2.addParentNode(linkB2, headNode);
        child3.addParentNode(linkB3, headNode);

        assertEquals(storeId, headNode.getNodeId());
        assertNull(headNode.getParents());
        assertEquals(3, headNode.getChildren().size());

        assertEquals(b1Id, child1.getNodeId());
        assertNull(child1.getChildren());
        assertNotNull(child1.getParents());
        assertEquals(1, child1.getParents().size());

    }
}
