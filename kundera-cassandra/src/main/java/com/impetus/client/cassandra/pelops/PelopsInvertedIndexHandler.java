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
package com.impetus.client.cassandra.pelops;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandlerBase;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Pelops implementation of {@link InvertedIndexHandler}
 * 
 * @author amresh.singh
 */
public class PelopsInvertedIndexHandler extends InvertedIndexHandlerBase implements InvertedIndexHandler
{
    @Override
    public void write(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler)
    {
        // Index in Inverted Index table if applicable
        boolean invertedIndexingApplicable = CassandraIndexHelper.isInvertedIndexingApplicable(entityMetadata);

        if (invertedIndexingApplicable)
        {

            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(entityMetadata.getTableName());

            Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(persistenceUnit));

            List<ThriftRow> indexThriftyRows = ((PelopsDataHandler) cdHandler).toIndexThriftRow(node.getData(),
                    entityMetadata, indexColumnFamily);

            for (ThriftRow thriftRow : indexThriftyRows)
            {
                mutator.writeColumns(indexColumnFamily, CassandraUtilities.toBytes(thriftRow.getId(),thriftRow.getId().getClass()),
                        Arrays.asList(thriftRow.getColumns().toArray(new Column[0])));

            }
            mutator.execute(consistencyLevel);
            indexThriftyRows = null;
        }
    }

    /**
     * @param columnFamilyName
     * @param m
     * @param filterClauseQueue
     * @return
     */
    @Override
    public List<SearchResult> search(EntityMetadata m, Queue<FilterClause> filterClauseQueue, String persistenceUnit,
            ConsistencyLevel consistencyLevel)
    {
        return super.search(m, filterClauseQueue, persistenceUnit, consistencyLevel);
    }

    /**
     * Searches <code>searchString</code> into <code>columnFamilyName</code>
     * (usually a wide row column family) for a given <code>rowKey</code> from
     * start to finish columns. Adds matching thrift columns into
     * <code>thriftColumns</code>
     * 
     * @param columnFamilyName
     * @param consistencyLevel
     * @param selector
     * @param rowKey
     * @param searchString
     * @param thriftColumns
     */
    @Override
    public void searchColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, String searchString, List<Column> thriftColumns, byte[] start,
            byte[] finish)
    {
        SlicePredicate colPredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(start);
        sliceRange.setFinish(finish);
        colPredicate.setSlice_range(sliceRange);

        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(persistenceUnit));
        List<Column> allThriftColumns = selector.getColumnsFromRow(columnFamilyName, rowKey, colPredicate,
                consistencyLevel);

        for (Column column : allThriftColumns)
        {
            String colName = Bytes.toUTF8(column.getName());
            // String colValue = Bytes.toUTF8(column.getValue());
            if (colName.indexOf(searchString) >= 0)
            {
                thriftColumns.add(column);
            }
        }
    }

    /**
     * Deletes records from inverted index table
     * 
     * @param entity
     * @param metadata
     */

    @Override
    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel)
    {
        super.delete(entity, metadata, consistencyLevel);
    }

    @Override
    public Column getColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName, String rowKey,
            String columnName, String persistenceUnit)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(persistenceUnit));
        Column thriftColumn = selector.getColumnFromRow(columnFamilyName, rowKey, columnName, consistencyLevel);
        return thriftColumn;
    }

    /**
     * @param mutator
     * @param indexColumnFamily
     * @param rowKey
     * @param columnName
     */
    public void deleteColumn(String indexColumnFamily, String rowKey, byte[] columnName, String persistenceUnit,
            ConsistencyLevel consistencyLevel)
    {
        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(persistenceUnit));
        mutator.deleteColumn(indexColumnFamily, rowKey, Bytes.fromByteArray(columnName));
        mutator.execute(consistencyLevel);
    }

}
