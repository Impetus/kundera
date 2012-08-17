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
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexingException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Thrift implementation of {@link InvertedIndexHandler}
 * @author amresh.singh
 */
public class ThriftInvertedIndexHandler implements InvertedIndexHandler
{
    
    Cassandra.Client cassandra_client;
    
    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftInvertedIndexHandler.class);
    
    
    public ThriftInvertedIndexHandler(Cassandra.Client cassandra_client) {
        this.cassandra_client = cassandra_client;
    }

    @Override
    public void writeToInvertedIndexTable(Node node, EntityMetadata entityMetadata, String persistenceUnit,
            ConsistencyLevel consistencyLevel, CassandraDataHandler cdHandler)
    {
        // Write to Inverted Index table if applicable
        boolean invertedIndexingApplicable = CassandraIndexHelper.isInvertedIndexingApplicable(entityMetadata);

        if (invertedIndexingApplicable)
        {
            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(entityMetadata.getTableName());
            
            ThriftDataHandler thriftDataHandler = (ThriftDataHandler) cdHandler;
            
            List<ThriftRow> indexThriftyRows = thriftDataHandler.toIndexThriftRow(node.getData(),
                    entityMetadata, indexColumnFamily);
            
            try
            {
                cassandra_client.set_keyspace(entityMetadata.getSchema());   
                
                
                for (ThriftRow thriftRow : indexThriftyRows)
                {
                    byte[] rowKey = Bytes.fromUTF8(thriftRow.getId()).toByteArray();
                    
                    //Create Insertion List
                    List<Mutation> insertion_list = new ArrayList<Mutation>();
                    
                    List<Column> thriftColumns = thriftRow.getColumns();
                    if(thriftColumns != null && !thriftColumns.isEmpty())
                    {
                        for(Column column : thriftColumns) {
                            Mutation mut = new Mutation();  
                            mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                            insertion_list.add(mut);
                        }                   
                    }
                    
                    //Create Mutation Map               
                    Map<String,List<Mutation>> columnFamilyValues = new HashMap<String,List<Mutation>>();               
                    columnFamilyValues.put(indexColumnFamily, insertion_list);              
                    Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();                
                    mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);    
                   
                    //Write Mutation map to database
                    cassandra_client.batch_mutate(mulationMap, consistencyLevel);
                }
            }
            catch (IllegalStateException e)
            {
                log.error(e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e); 
            }
            catch (InvalidRequestException e)
            {
                log.error(e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e); 
            }
            catch (TException e)
            {
                log.error(e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e); 
            }
            catch (UnavailableException e)
            {
                log.error(e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e); 
            }
            catch (TimedOutException e)
            {
                log.error(e.getMessage());
                throw new IndexingException("Unable to insert records into inverted index", e); 
            }           
        }
        
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
