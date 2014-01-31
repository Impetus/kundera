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
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.NotFoundException;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandlerBase;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * Pelops implementation of {@link InvertedIndexHandler}
 * 
 * @author amresh.singh
 */
public class PelopsInvertedIndexHandler extends InvertedIndexHandlerBase implements InvertedIndexHandler
{
    private static final Logger log = LoggerFactory.getLogger(PelopsInvertedIndexHandler.class);

    private final PelopsClient pelopsClient;

    /**
     * @param externalProperties
     */
    public PelopsInvertedIndexHandler(final PelopsClient pelopsClient, final boolean useSecondryIndex)
    {
        this.pelopsClient = pelopsClient;
        this.useSecondryIndex = useSecondryIndex;
    }

    @Override
    public void write(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler)
    {
        // Index in Inverted Index table if applicable
        boolean invertedIndexingApplicable = CassandraIndexHelper.isInvertedIndexingApplicable(entityMetadata,useSecondryIndex);

        if (invertedIndexingApplicable)
        {
            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(entityMetadata.getTableName());

            Mutator mutator = pelopsClient.getMutator();

            List<ThriftRow> indexThriftyRows = ((PelopsDataHandler) cdHandler).toIndexThriftRow(node.getData(),
                    entityMetadata, indexColumnFamily);

            for (ThriftRow thriftRow : indexThriftyRows)
            {

                List<Column> thriftColumns = thriftRow.getColumns();
                List<SuperColumn> thriftSuperColumns = thriftRow.getSuperColumns();
                if (thriftColumns != null && !thriftColumns.isEmpty())
                {
                    // Bytes.fromL
                    mutator.writeColumns(thriftRow.getColumnFamilyName(),
                            CassandraUtilities.toBytes(thriftRow.getId(), thriftRow.getId().getClass()),
                            Arrays.asList(thriftRow.getColumns().toArray(new Column[0])));
                }

                if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
                {
                    for (SuperColumn sc : thriftSuperColumns)
                    {
                        mutator.writeSubColumns(thriftRow.getColumnFamilyName(),
                                CassandraUtilities.toBytes(thriftRow.getId(), thriftRow.getId().getClass()),
                                Bytes.fromByteArray(sc.getName()), sc.getColumns());
                    }
                }
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
    public List<SearchResult> search(EntityMetadata m, String persistenceUnit, ConsistencyLevel consistencyLevel,
            Map<Boolean, List<IndexClause>> indexClauseMap)
    {
        return super.search(m, persistenceUnit, consistencyLevel, indexClauseMap);
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
     * @param thriftSuperColumns
     */
    @Override
    public void searchSuperColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, byte[] searchSuperColumnName, List<SuperColumn> thriftSuperColumns,
            byte[] start, byte[] finish)
    {
        SlicePredicate colPredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(start);
        sliceRange.setFinish(finish);
        colPredicate.setSlice_range(sliceRange);

        Selector selector = pelopsClient.getSelector();
        List<SuperColumn> allThriftSuperColumns = selector.getSuperColumnsFromRow(columnFamilyName, rowKey,
                colPredicate, consistencyLevel);

        for (SuperColumn superColumn : allThriftSuperColumns)
        {
            if (superColumn == null)
                continue;
            if (superColumn.getName() == searchSuperColumnName)
            {
                thriftSuperColumns.add(superColumn);
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
    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel, final KunderaMetadata kunderaMetadata)
    {
        super.delete(entity, metadata, consistencyLevel, kunderaMetadata);
    }

    @Override
    public SuperColumn getSuperColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName, String rowKey,
            byte[] superColumnName, String persistenceUnit)
    {
        Selector selector = pelopsClient.getSelector();
        SuperColumn thriftSuperColumn = null;
        try
        {
            thriftSuperColumn = selector.getSuperColumnFromRow(columnFamilyName, rowKey,
                    Bytes.fromByteArray(superColumnName), consistencyLevel);

        }
        catch (NotFoundException e)
        {
            log.warn("Error while fetching super column for Row {} , Caused by: .",rowKey, e);
            return null;
        }
        catch (PelopsException e)
        {
            log.warn("Error while fetching super column for Row {} , Caused by: .",rowKey, e);
            return null;
        }
        return thriftSuperColumn;
    }

    /**
     * @param indexColumnFamily
     * @param rowKey
     * @param superColumnName
     * @param mutator
     */
    public void deleteColumn(String indexColumnFamily, String rowKey, byte[] superColumnName, String persistenceUnit,
            ConsistencyLevel consistencyLevel, byte[] columnName)
    {
        Mutator mutator = pelopsClient.getMutator();
        mutator.deleteColumn(indexColumnFamily, rowKey, Bytes.fromByteArray(superColumnName));
        mutator.execute(consistencyLevel);
    }
}
