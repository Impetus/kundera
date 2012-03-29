/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;

/**
 * The Interface EntityReader.
 * 
 * @author vivek.mishra 
 * 
 * Interface to provide declarations for methods
 *         responsible for entity read operations.(Except queries).
 */
public interface EntityReader
{

    /**
     * Method responsible for reading back entity and relations using secondary
     * indexes(if it holds any relation), else retrieve row keys using lucene.
     * 
     * @param m
     *            entity meta data
     * @param relationNames
     *            relation names
     * @param isParent
     *            if entity is not holding any relation.
     * @param client
     *            client instance
     * @return list of wrapped enhance entities.
     */
    List<EnhanceEntity> populateRelation(EntityMetadata m, List<String> relationNames, boolean isParent, Client client);

    /**
     * Returns populated entity along with all relational value.
     * 
     * @param e
     *            enhance entity
     * @param graphs
     *            entity graph
     * @param collectionHolder
     *            collection holder.
     * @param client
     *            client
     * @param m
     *            entity meta data
     * @param persistenceDelegeator
     *            persistence delegator.
     * @return populate entity.
     * @throws Exception
     *             the exception
     */
    
    Object recursivelyFindEntities(EnhanceEntity e, Client client, EntityMetadata m, PersistenceDelegator pd);
    
    
    @Deprecated
    Object computeGraph(EnhanceEntity e, List<EntitySaveGraph> graphs, Map<Object, Object> collectionHolder,
            Client client, EntityMetadata m, PersistenceDelegator persistenceDelegeator); 

    /**
     * Find by id.
     * 
     * @param primaryKey
     *            the primary key
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param client
     *            the client
     * @return the enhance entity
     */
    EnhanceEntity findById(Object primaryKey, EntityMetadata m, List<String> relationNames, Client client);

    
}
