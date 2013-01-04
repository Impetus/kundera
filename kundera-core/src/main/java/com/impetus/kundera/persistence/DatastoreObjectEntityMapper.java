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
package com.impetus.kundera.persistence;

import java.util.List;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Defines methods for converting JPA entity to objects represented in underlying datastore, and vice versa.
 * @author amresh.singh
 */
public interface DatastoreObjectEntityMapper
{
    
    /**
     * Converts an entity object to objects represented in underlying datastore
     * @param entity
     * @param relations
     * @param m
     * @return
     */
    public Object fromEntity(Object entity, Object datastoreObject, List<RelationHolder> relations, EntityMetadata m);

    /**
     * Converts datastore specific objects to JPA entity
     * @param datastoreObject
     * @param relationNames
     * @param m
     * @return
     */
    public Object toEntity(Object datastoreObject, List<String> relationNames, EntityMetadata m);

}
