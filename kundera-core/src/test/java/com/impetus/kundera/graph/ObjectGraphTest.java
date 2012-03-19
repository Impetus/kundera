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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.graph.NodeLink.LinkProperty;

/**
 * Test case for {@link ObjectGraph}
 * @author amresh.singh
 */
public class ObjectGraphTest
{
    ObjectGraph objectGraph;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        objectGraph = new ObjectGraph();
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
        Person person = new Person();
        Address a1 = new Address();
        Address a2 = new Address();
        Address a3 = new Address();
        
        String personId = ObjectGraphBuilder.getNodeId("1", person);
        String a1Id = ObjectGraphBuilder.getNodeId("A1", a1);
        String a2Id = ObjectGraphBuilder.getNodeId("A2", a2);
        String a3Id = ObjectGraphBuilder.getNodeId("A3", a3);
        
        Node headNode = new Node(personId, person);       
        
        Node child1 = new Node(a1Id, a1);
        Node child2 = new Node(a2Id, a2);
        Node child3 = new Node(a3Id, a3);
        
        NodeLink linkA1 = new NodeLink(personId, a1Id);
        NodeLink linkA2 = new NodeLink(personId, a2Id);
        NodeLink linkA3 = new NodeLink(personId, a3Id);
        
        linkA1.addLinkProperty(LinkProperty.JOIN_COLUMN_NAME, "PERSON_ID");
        linkA2.addLinkProperty(LinkProperty.JOIN_COLUMN_NAME, "PERSON_ID");
        linkA3.addLinkProperty(LinkProperty.JOIN_COLUMN_NAME, "PERSON_ID");
        
        headNode.addChildNode(linkA1, child1);
        headNode.addChildNode(linkA2, child2);
        headNode.addChildNode(linkA3, child3);
        
        child1.addParentNode(linkA1, headNode);
        child2.addParentNode(linkA2, headNode);
        child3.addParentNode(linkA3, headNode);
        
        assertEquals(personId, headNode.getNodeId());
        assertNull(headNode.getParents());
        assertEquals(3, headNode.getChildren().size());
        
        assertEquals(a1Id, child1.getNodeId());
        assertNull(child1.getChildren());
        assertNotNull(child1.getParents());
        assertEquals(1, child1.getParents().size());
        
    }

}
