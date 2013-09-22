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
package com.impetus.kundera.proxy.collection;

import java.util.Map;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Top level interface for all proxy collections (including maps)
 * 
 * @author amresh.singh
 */
public interface ProxyCollection
{

    /**
     * Returns owning entity that holds this proxy collection
     */
    Object getOwner();

    /**
     * Sets owning entity that holds this collection for later use while eagerly
     * loading
     */
    void setOwner(Object owner);

    /**
     * Returns instance of {@link PersistenceDelegator} which is required for
     * eagerly loading associated collection at the time it is requested
     */
    PersistenceDelegator getPersistenceDelegator();

    /**
     * Retrieves {@link Map} of Relation name and values required for eagerly
     * fetching this proxy collection at the time it is requested
     */
    Map<String, Object> getRelationsMap();

    /**
     * Sets {@link Map} of Relation name and values required for eagerly
     * fetching this proxy collection at the time it is requested
     */
    void setRelationsMap(Map<String, Object> relationsMap);

    /**
     * Instance of {@link Relation} corresponding to this proxy collection
     */
    Relation getRelation();

    /**
     * Retrieves actual collection data that implementor of this interface
     * holds. Unless proxy collection is eagerly loaded, this values would
     * normally be null
     */
    Object getDataCollection();

    /**
     * Retrieves a copy of this proxy collection
     */
    ProxyCollection getCopy();
}
