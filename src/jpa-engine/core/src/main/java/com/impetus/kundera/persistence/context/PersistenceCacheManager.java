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
package com.impetus.kundera.persistence.context;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * @author amresh
 * 
 */
public class PersistenceCacheManager
{
    private PersistenceCache persistenceCache;

    public PersistenceCacheManager(PersistenceCache pc)
    {
        this.persistenceCache = pc;
    }

    public void clearPersistenceCache()
    {
        persistenceCache.clean();

    } // cleanIndividualCache(pc.getMainCache());
      // cleanIndividualCache(pc.getEmbeddedCache());
      // cleanIndividualCache(pc.getElementCollectionCache());
      // cleanIndividualCache(pc.getTransactionalCache());

    private void cleanIndividualCache(CacheBase cache)
    {
        for (Node node : cache.getAllNodes())
        {
            node.clear();
        }
    }

    public void markAllNodesNotTraversed()
    {
        for (Node node : persistenceCache.getMainCache().getAllNodes())
        {
            node.setTraversed(false);
        }

/*        for (Node node : persistenceCache.getEmbeddedCache().getAllNodes())
        {
            node.setTraversed(false);
        }

        for (Node node : persistenceCache.getElementCollectionCache().getAllNodes())
        {
            node.setTraversed(false);
        }

        for (Node node : persistenceCache.getTransactionalCache().getAllNodes())
        {
            node.setTraversed(false);
        }*/
    }

    /**
     * @param entity
     * @param pd
     * @param entityId
     */
    public static void addEntityToPersistenceCache(Object entity, PersistenceDelegator pd, Object entityId)
    {
        MainCache mainCache = (MainCache) pd.getPersistenceCache().getMainCache();
        String nodeId = ObjectGraphUtils.getNodeId(entityId, entity.getClass());
        Node node = new Node(nodeId, entity.getClass(), new ManagedState(), pd.getPersistenceCache(), entityId, pd);
        node.setData(entity);
        node.setPersistenceDelegator(pd);
        mainCache.addNodeToCache(node);
    }

}
