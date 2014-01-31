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
package com.impetus.kundera.graph;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;

import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh.singh
 */
public class StoreBuilder
{

    public static Node buildStoreNode(PersistenceCache pc, NodeState initialState, CascadeType cascadeType)
    {

        Store store = new Store();
        BillingCounter b1 = new BillingCounter();
        BillingCounter b2 = new BillingCounter();
        BillingCounter b3 = new BillingCounter();

        String storeId = ObjectGraphUtils.getNodeId("1", store.getClass());
        String b1Id = ObjectGraphUtils.getNodeId("A1", b1.getClass());
        String b2Id = ObjectGraphUtils.getNodeId("A2", b2.getClass());
        String b3Id = ObjectGraphUtils.getNodeId("A3", b3.getClass());

        Node headNode = new Node(storeId, store, initialState, pc, "1", null);

        Node child1 = new Node(b1Id, b1, initialState, pc, "A1", null);
        Node child2 = new Node(b2Id, b2, initialState, pc, "A2", null);
        Node child3 = new Node(b3Id, b3, initialState, pc, "A3", null);

        NodeLink linkB1 = new NodeLink(storeId, b1Id);
        NodeLink linkB2 = new NodeLink(storeId, b2Id);
        NodeLink linkB3 = new NodeLink(storeId, b3Id);

        List<CascadeType> cascadeTypes = new ArrayList<CascadeType>();
        cascadeTypes.add(cascadeType);

        linkB1.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");
        linkB1.addLinkProperty(LinkProperty.CASCADE, cascadeTypes);
        linkB2.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");
        linkB2.addLinkProperty(LinkProperty.CASCADE, cascadeTypes);
        linkB3.addLinkProperty(LinkProperty.LINK_NAME, "STORE_ID");
        linkB3.addLinkProperty(LinkProperty.CASCADE, cascadeTypes);

        headNode.addChildNode(linkB1, child1);
        headNode.addChildNode(linkB2, child2);
        headNode.addChildNode(linkB3, child3);

        child1.addParentNode(linkB1, headNode);
        child2.addParentNode(linkB2, headNode);
        child3.addParentNode(linkB3, headNode);

        return headNode;
    }

}
