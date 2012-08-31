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
import java.util.Queue;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

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
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Thrift implementation of {@link InvertedIndexHandler}
 * 
 * @author amresh.singh
 */
public class ThriftInvertedIndexHandler extends InvertedIndexHandlerBase implements InvertedIndexHandler
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftInvertedIndexHandler.class);

    public ThriftInvertedIndexHandler()
    {        
    }

    @Override
    public void write(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler)
    {
        // Write to Inverted Index table if applicable
        boolean invertedIndexingApplicable = CassandraIndexHelper.isInvertedIndexingApplicable(entityMetadata);

        if (invertedIndexingApplicable)
        {
            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(entityMetadata.getTableName());

            ThriftDataHandler thriftDataHandler = (ThriftDataHandler) cdHandler;

            List<ThriftRow> indexThriftyRows = thriftDataHandler.toIndexThriftRow(node.getData(), entityMetadata,
                    indexColumnFamily);
            IPooledConnection conn = null;
            try
            {
                String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);
                
                conn = PelopsUtils.getCassandraConnection(persistenceUnit);
                Cassandra.Client cassandra_client = conn.getAPI();              
                
                cassandra_client.set_keyspace(keyspace);                

                for (ThriftRow thriftRow : indexThriftyRows)
                {
                    byte[] rowKey = PropertyAccessorHelper.toBytes(thriftRow.getId(),thriftRow.getId().getClass());

                    // Create Insertion List
                    List<Mutation> insertion_list = new ArrayList<Mutation>();

                    List<Column> thriftColumns = thriftRow.getColumns();
                    if (thriftColumns != null && !thriftColumns.isEmpty())
                    {
                        for (Column column : thriftColumns)
                        {
                            Mutation mut = new Mutation();
                            mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                            insertion_list.add(mut);
                        }
                    }

                    // Create Mutation Map
                    Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                    columnFamilyValues.put(indexColumnFamily, insertion_list);
                    Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                    mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

                    // Write Mutation map to database
                    cassandra_client.batch_mutate(mulationMap, consistencyLevel);
                }
            }
            catch (IllegalStateException e)
            {
                log.error("Unable to insert records into inverted index. Details:" + e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (InvalidRequestException e)
            {
                log.error("Unable to insert records into inverted index. Details:" + e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (TException e)
            {
                log.error("Unable to insert records into inverted index. Details:" + e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (UnavailableException e)
            {
                log.error("Unable to insert records into inverted index. Details:" + e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            catch (TimedOutException e)
            {
                log.error("Unable to insert records into inverted index. Details:" + e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e);
            }
            finally
            {
                PelopsUtils.releaseConnection(conn);
            }
        }

    }

    @Override
    public List<SearchResult> search(EntityMetadata m, Queue<FilterClause> filterClauseQueue, String persistenceUnit,
            ConsistencyLevel consistencyLevel)
    {

        return super.search(m, filterClauseQueue, persistenceUnit, consistencyLevel);
    }

    @Override
    protected void searchColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, String searchString, List<Column> thriftColumns, byte[] start,
            byte[] finish)
    {
        SlicePredicate colPredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(start);
        sliceRange.setFinish(finish);
        colPredicate.setSlice_range(sliceRange);

        List<ColumnOrSuperColumn> coscList = null;
        IPooledConnection conn = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);
            conn = PelopsUtils.getCassandraConnection(persistenceUnit);
            Cassandra.Client cassandra_client = conn.getAPI();    
            
            cassandra_client.set_keyspace(keyspace);
            coscList = cassandra_client.get_slice(ByteBuffer.wrap(rowKey.getBytes()),
                    new ColumnParent(columnFamilyName), colPredicate, consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (UnavailableException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        } 
        finally
        {
            PelopsUtils.releaseConnection(conn);
        }

        List<Column> allThriftColumns = ThriftDataResultHelper.transformThriftResult(coscList, ColumnFamilyType.COLUMN,null);

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

    @Override
    protected Column getColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName, String rowKey,
            String columnName, String persistenceUnit)
    {
        ColumnPath cp = new ColumnPath(columnFamilyName);
        cp.setColumn(columnName.getBytes());
        ColumnOrSuperColumn cosc;
        
        IPooledConnection conn = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);
            
            conn = PelopsUtils.getCassandraConnection(persistenceUnit);
            Cassandra.Client cassandra_client = conn.getAPI();             
            
            cassandra_client.set_keyspace(keyspace);
            cosc = cassandra_client.get(ByteBuffer.wrap(rowKey.getBytes()), cp, consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (NotFoundException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (UnavailableException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to search from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to search from inverted index", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(conn);
        }

        Column thriftColumn = ThriftDataResultHelper.transformThriftResult(cosc, ColumnFamilyType.COLUMN,null);
        return thriftColumn;
    }

    @Override
    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel)
    {

        super.delete(entity, metadata, consistencyLevel);

    }

    @Override
    protected void deleteColumn(String indexColumnFamily, String rowKey, byte[] columnName, String persistenceUnit,
            ConsistencyLevel consistencyLevel)
    {
        ColumnPath cp = new ColumnPath(indexColumnFamily);
        cp.setColumn(columnName);

        IPooledConnection conn = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);
            
            conn = PelopsUtils.getCassandraConnection(persistenceUnit);
            Cassandra.Client cassandra_client = conn.getAPI();              
            
            cassandra_client.set_keyspace(keyspace);
            cassandra_client.remove(ByteBuffer.wrap(rowKey.getBytes()), cp, System.currentTimeMillis(),
                    consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Unable to delete data from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (UnavailableException e)
        {
            log.error("Unable to delete data from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (TimedOutException e)
        {
            log.error("Unable to delete data from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        catch (TException e)
        {
            log.error("Unable to delete data from inverted index. Details:" + e.getMessage());
            throw new IndexingException("Unable to delete data from inverted index", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(conn);
        }
    }

}
