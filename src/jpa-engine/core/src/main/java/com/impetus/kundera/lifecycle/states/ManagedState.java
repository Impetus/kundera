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
package com.impetus.kundera.lifecycle.states;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.PersistenceContextType;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.NodeStateContext;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author amresh
 * 
 */
public class ManagedState extends NodeState
{

    @Override
    public void initialize(NodeStateContext nodeStateContext)
    {

    }

    @Override
    public void handlePersist(NodeStateContext nodeStateContext)
    {
        // Ignored, entity remains in the same state

        // Cascade persist operation for related entities for whom cascade=ALL
        // or PERSIST
//        if (((Node) nodeStateContext).isDirty())
//        {
//            recursivelyPerformOperation(nodeStateContext, OPERATION.PERSIST);
//        }
    }

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        // Managed ---> Removed
        moveNodeToNextState(nodeStateContext, new RemovedState());

        // Mark entity for removal in persistence context
        nodeStateContext.setDirty(true);

        // Recurse remove operation for all related entities for whom
        // cascade=ALL or REMOVE
//        recursivelyPerformOperation(nodeStateContext, OPERATION.REMOVE);
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        // Refresh entity state from the database
        // Fetch Node data from Client
        Client client = nodeStateContext.getClient();
        Class<?> nodeDataClass = nodeStateContext.getDataClass();
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(nodeStateContext.getPersistenceDelegator().getKunderaMetadata(), nodeDataClass);
        Object entityId = nodeStateContext.getEntityId();

        EntityReader reader = client.getReader();
        EnhanceEntity ee = reader.findById(entityId, entityMetadata, client);

        if (ee != null && ee.getEntity() != null)
        {
            Object nodeData = ee.getEntity();
            nodeStateContext.setData(nodeData);
        }

        // Cascade refresh operation for all related entities for whom
        // cascade=ALL or REFRESH
        recursivelyPerformOperation(nodeStateContext, OPERATION.REFRESH);
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
        // Ignored, entity remains in the same state

        // Mark this entity for saving in database depending upon whether it's
        // deep equals to the
        // one in persistence cache
        // nodeStateContext.setDirty(true);

//        if (((Node) nodeStateContext).isDirty() || ((Node) nodeStateContext).isInState(DetachedState.class))
//        {
            ((Node) nodeStateContext).setUpdate(true);
            // Add this node into persistence cache
            nodeStateContext.getPersistenceCache().getMainCache().addNodeToCache((Node) nodeStateContext);

            // Cascade merge operation for all related entities for whom
            // cascade=ALL
            // or MERGE
//            recursivelyPerformOperation(nodeStateContext, OPERATION.MERGE);

//        }
    }

    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
        // Fetch Node data from Client
        Client client = nodeStateContext.getClient();
        Class<?> nodeDataClass = nodeStateContext.getDataClass();
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(nodeStateContext.getPersistenceDelegator().getKunderaMetadata(), nodeDataClass);
        Object entityId = nodeStateContext.getEntityId();

        Object nodeData = null; // Node data

        EntityReader reader = client.getReader();
        if (reader == null)
        {
            return;
        }

        EnhanceEntity ee = reader.findById(entityId, entityMetadata, client);
        // Recursively retrieve relationship entities (if there are any)
        if (ee != null && ee.getEntity() != null)
        {
            Object entity = ee.getEntity();

            if ((entityMetadata.getRelationNames() == null || entityMetadata.getRelationNames().isEmpty())
                    && !entityMetadata.isRelationViaJoinTable())
            {
                // There is no relation (not even via Join Table), Construct
                // Node out of this enhance entity,
                nodeData = entity;
            }

            else
            {
                // This entity has associated entities, find them recursively.
                Map<Object, Object> relationStack = new HashMap<Object, Object>();
                relationStack.put(nodeDataClass.getCanonicalName()+"#"+PropertyAccessorHelper.getId(entity, entityMetadata), entity);
                nodeData = reader.recursivelyFindEntities(ee.getEntity(), ee.getRelations(), entityMetadata,
                        nodeStateContext.getPersistenceDelegator(), false,relationStack);
            }
        }

        // Construct Node out of this entity and put into Persistence Cache
        if (nodeData != null)
        {

            nodeStateContext.setData(nodeData);
            nodeStateContext.getPersistenceCache().getMainCache().processNodeMapping((Node) nodeStateContext);

            // This node is fresh and hence NOT dirty
            nodeStateContext.setDirty(false);
            // One time set as required for rollback.
            Object original = ((Node) nodeStateContext).clone();
            ((Node) nodeStateContext).setOriginalNode((Node) original);
        }

        // No state change, Node to remain in Managed state
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
        handleDetach(nodeStateContext);
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
        handleDetach(nodeStateContext);
    }

    @Override
    public void handleFlush(NodeStateContext nodeStateContext)
    {
        // Entity state to remain as Managed

        // Flush this node to database
        Client client = nodeStateContext.getClient();
        client.persist((Node) nodeStateContext);

        // logNodeEvent("FLUSHED", this, nodeStateContext.getNodeId());

        // Since node is flushed, mark it as NOT dirty
        nodeStateContext.setDirty(false);

    }

    @Override
    public void handleLock(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleDetach(NodeStateContext nodeStateContext)
    {
        // Managed ---> Detached
        moveNodeToNextState(nodeStateContext, new DetachedState());

        // Cascade detach operation to all referenced entities for whom
        // cascade=ALL or DETACH
        recursivelyPerformOperation(nodeStateContext, OPERATION.DETACH);
    }

    @Override
    public void handleCommit(NodeStateContext nodeStateContext)
    {
        nodeStateContext.setCurrentNodeState(new DetachedState());
    }

    @Override
    public void handleRollback(NodeStateContext nodeStateContext)
    {
        // If persistence context is EXTENDED, Next state should be Transient
        // If persistence context is TRANSACTIONAL, Next state should be
        // detached

        if (PersistenceContextType.EXTENDED.equals(nodeStateContext.getPersistenceCache().getPersistenceContextType()))
        {
            moveNodeToNextState(nodeStateContext, new TransientState());

        }
        else if (PersistenceContextType.TRANSACTION.equals(nodeStateContext.getPersistenceCache()
                .getPersistenceContextType()))
        {
            moveNodeToNextState(nodeStateContext, new DetachedState());
        }
    }

    @Override
    public void handleGetReference(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleContains(NodeStateContext nodeStateContext)
    {
    }

}