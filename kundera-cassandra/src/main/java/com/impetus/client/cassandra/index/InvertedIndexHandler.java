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
package com.impetus.client.cassandra.index;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * This interface defines methods for operation on inverted indexes
 * 
 * @author amresh.singh
 */
public interface InvertedIndexHandler
{

    /**
     * Writes a record into inverted index table.
     * @param node
     * @param entityMetadata
     * @param persistenceUnit
     * @param consistencyLevel
     * @param cdHandler
     */
    void write(Node node, EntityMetadata entityMetadata, String persistenceUnit, ConsistencyLevel consistencyLevel,
            CassandraDataHandler cdHandler);

    /**
     * Searches records from Inverted index table.
     * @param m
     * @param filterClauseQueue
     * @param persistenceUnit
     * @param consistencyLevel
     * @return
     */
    List<SearchResult> search(EntityMetadata m, String persistenceUnit, ConsistencyLevel consistencyLevel, Map<Boolean, List<IndexClause>> indexClauseMap);

    /**
     * Deletes a record from inverted index table.
     * @param entity
     * @param metadata
     * @param consistencyLevel
     */
    void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel);

}
