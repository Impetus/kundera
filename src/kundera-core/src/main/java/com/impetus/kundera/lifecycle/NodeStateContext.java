/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.lifecycle;

import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * State context of a given entity
 * 
 * @author amresh
 * 
 */
public interface NodeStateContext
{
    // State methods
    NodeState getCurrentNodeState();

    void setCurrentNodeState(NodeState nodeState);

    String getNodeId();

    void setNodeId(String nodeId);

    Object getData();

    void setData(Object data);

    Class getDataClass();

    void setDataClass(Class dataClass);

    Map<NodeLink, Node> getParents();

    void setParents(Map<NodeLink, Node> parents);

    Map<NodeLink, Node> getChildren();

    void setChildren(Map<NodeLink, Node> children);

    Node getParentNode(String parentNodeId);

    Node getChildNode(String childNodeId);

    void addParentNode(NodeLink nodeLink, Node node);

    void addChildNode(NodeLink nodeLink, Node node);

    boolean isTraversed();

    void setTraversed(boolean traversed);

    boolean isDirty();

    void setDirty(boolean dirty);

    boolean isHeadNode();

    Client getClient();

    void setClient(Client client);

    PersistenceDelegator getPersistenceDelegator();

    void setPersistenceDelegator(PersistenceDelegator pd);

    // Life cycle Management
    void persist();

    void remove();

    void refresh();

    void merge();

    void detach();

    void close();

    void lock();

    void commit();

    void rollback();

    // Identity Management
    void find();

    void getReference();

    void contains();

    // Cache Management
    void clear();

    void flush();

    public boolean isInState(Class<?> stateClass);

    public PersistenceCache getPersistenceCache();

    public void setPersistenceCache(PersistenceCache persistenceCache);

    public Object getEntityId();
}
