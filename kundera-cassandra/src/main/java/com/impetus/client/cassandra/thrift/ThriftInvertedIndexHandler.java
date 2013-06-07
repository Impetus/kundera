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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.dataforte.cassandra.pool.ConnectionPool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandlerBase;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexingException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Thrift implementation of {@link InvertedIndexHandler}
 * 
 * @author amresh.singh
 */
public class ThriftInvertedIndexHandler extends InvertedIndexHandlerBase implements InvertedIndexHandler
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftInvertedIndexHandler.class);

    private ConnectionPool pool;

    public ThriftInvertedIndexHandler(ConnectionPool pool)
    {
        this.pool = pool;
    }

    @Override
    public void write(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler, Object conn)
    {
        // Write to Inverted Index table if applicable
        boolean invertedIndexingApplicable = CassandraIndexHelper.isInvertedIndexingApplicable(entityMetadata);

        if (invertedIndexingApplicable)
        {
            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(entityMetadata.getTableName());

            ThriftDataHandler thriftDataHandler = (ThriftDataHandler) cdHandler;

            List<ThriftRow> indexThriftyRows = thriftDataHandler.toIndexThriftRow(node.getData(), entityMetadata,
                    indexColumnFamily);
            try
            {
                String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);

                for (ThriftRow thriftRow : indexThriftyRows)
                {
                    byte[] rowKey = PropertyAccessorHelper.toBytes(thriftRow.getId(), thriftRow.getId().getClass());

                    // Create Insertion List
                    List<Mutation> insertion_list = new ArrayList<Mutation>();

                    List<Column> thriftColumns = thriftRow.getColumns();
                    List<SuperColumn> thriftSuperColumns = thriftRow.getSuperColumns();
                    if (thriftColumns != null && !thriftColumns.isEmpty())
                    {
                        for (Column column : thriftColumns)
                        {
                            Mutation mut = new Mutation();
                            mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                            insertion_list.add(mut);
                        }
                    }

                    if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
                    {
                        for (SuperColumn superColumn : thriftSuperColumns)
                        {
                            Mutation mut = new Mutation();
                            mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(superColumn));
                            insertion_list.add(mut);
                        }
                    }

                    // Create Mutation Map
                    Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                    columnFamilyValues.put(indexColumnFamily, insertion_list);
                    Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                    mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

                    // Write Mutation map to database
                    ((Cassandra.Client) conn).batch_mutate(mulationMap, consistencyLevel);
                }
            }
            catch (IllegalStateException e)
            {
                log.error("Unable to insert records into inverted index, Caused by: ", e);
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (InvalidRequestException e)
            {
                log.error("Unable to insert records into inverted index, Caused by: ", e);
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (TException e)
            {
                log.error("Unable to insert records into inverted index, Caused by: ", e);
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (UnavailableException e)
            {
                log.error("Unable to insert records into inverted index, Caused by: ", e);
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (TimedOutException e)
            {
                log.error("Unable to insert records into inverted index, Caused by: ", e);
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
        }
    }

    @Override
    public List<SearchResult> search(EntityMetadata m, String persistenceUnit, ConsistencyLevel consistencyLevel,
            Map<Boolean, List<IndexClause>> indexClauseMap, Object conn)
    {

        return super.search(m, persistenceUnit, consistencyLevel, indexClauseMap, conn);
    }

    @Override
    protected void searchSuperColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, byte[] searchSuperColumnName, List<SuperColumn> thriftSuperColumns,
            byte[] start, byte[] finish, Object conn)
    {
        SlicePredicate colPredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(start);
        sliceRange.setFinish(finish);
        colPredicate.setSlice_range(sliceRange);

        List<ColumnOrSuperColumn> coscList = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);
            coscList = ((Cassandra.Client) conn).get_slice(ByteBuffer.wrap(rowKey.getBytes()), new ColumnParent(
                    columnFamilyName), colPredicate, consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (UnavailableException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }

        List<SuperColumn> allThriftSuperColumns = ThriftDataResultHelper.transformThriftResult(coscList,
                ColumnFamilyType.SUPER_COLUMN, null);

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

    @Override
    protected SuperColumn getSuperColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName,
            String rowKey, byte[] superColumnName, String persistenceUnit, Object conn)
    {
        ColumnPath cp = new ColumnPath(columnFamilyName);
        cp.setSuper_column(superColumnName);
        ColumnOrSuperColumn cosc = null;

        try
        {
            cosc = ((Cassandra.Client) conn).get(ByteBuffer.wrap(rowKey.getBytes()), cp, consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (NotFoundException e)
        {
            log.warn("Not found any record in inverted index table");
        }
        catch (UnavailableException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to search from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to search from inverted index", e);
        }

        SuperColumn thriftSuperColumn = ThriftDataResultHelper.transformThriftResult(cosc,
                ColumnFamilyType.SUPER_COLUMN, null);
        return thriftSuperColumn;
    }

    @Override
    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel, Object conn)
    {

        super.delete(entity, metadata, consistencyLevel, conn);

    }

    @Override
    protected void deleteColumn(String indexColumnFamily, String rowKey, byte[] superColumnName,
            String persistenceUnit, ConsistencyLevel consistencyLevel, byte[] columnName, Object conn)
    {
        ColumnPath cp = new ColumnPath(indexColumnFamily);
        cp.setSuper_column(superColumnName);

        try
        {
            ColumnOrSuperColumn cosc;
            try
            {
                cosc = ((Cassandra.Client) conn).get(ByteBuffer.wrap(rowKey.getBytes()), cp, consistencyLevel);
            }
            catch (NotFoundException e)
            {
                return;
            }
            SuperColumn thriftSuperColumn = ThriftDataResultHelper.transformThriftResult(cosc,
                    ColumnFamilyType.SUPER_COLUMN, null);

            if (thriftSuperColumn != null && thriftSuperColumn.getColumns() != null
                    && thriftSuperColumn.getColumns().size() > 1)
            {
                cp.setColumn(columnName);
            }

            ((Cassandra.Client) conn).remove(ByteBuffer.wrap(rowKey.getBytes()), cp, System.currentTimeMillis(),
                    consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to delete data from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (UnavailableException e)
        {
            log.error("Unable to delete data from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to delete data from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to delete data from inverted index, Caused by: ", e);
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
    }
}
