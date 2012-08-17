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
package com.impetus.client.cassandra.thrift;

import java.util.List;
import java.util.Queue;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Thrift implementation of {@link InvertedIndexHandler}
 * @author amresh.singh
 */
public class ThriftInvertedIndexHandler implements InvertedIndexHandler
{

    @Override
    public void writeToInvertedIndexTable(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler)
    {
    }

    @Override
    public List<SearchResult> getSearchResults(EntityMetadata m, Queue<FilterClause> filterClauseQueue,
            String persistenceUnit, ConsistencyLevel consistencyLevel)
    {
        return null;
    }

    @Override
    public void deleteRecordsFromIndexTable(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel)
    {
    }   

}
