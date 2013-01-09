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
package com.impetus.client.neo4j;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;

import com.impetus.client.neo4j.index.AutoIndexing;
import com.impetus.client.neo4j.query.Neo4JQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * Implementation of {@link Client} using Neo4J Native Java driver 
 * (see Embedded Java driver at http://www.neo4j.org/develop/drivers)  
 * @author amresh.singh
 */
public class Neo4JClient extends Neo4JClientBase implements Client<Neo4JQuery>
{
    
    private static Log log = LogFactory.getLog(Neo4JClient.class);

    /**
     * Reference to Neo4J client factory.
     */
    private Neo4JClientFactory factory;

    private EntityReader reader;
    
    private GraphEntityMapper mapper;
    
    
    Neo4JClient(final Neo4JClientFactory factory)
    {
        this.factory = factory;
        reader = new Neo4JEntityReader();
        mapper = new GraphEntityMapper();
    }
    
    
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        
        GraphDatabaseService graphDb = factory.getConnection();
        AutoIndexing autoIndexing = new AutoIndexing();
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);
        String idColumnName = ((AbstractAttribute)m.getIdAttribute()).getJPAColumnName();
        
        Object entity = null;
        
        if(autoIndexing.isNodeAutoIndexingEnabled(graphDb))
        {
            // Get the Node auto index
            ReadableIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            IndexHits<Node> nodesFound = autoNodeIndex.get(idColumnName, key);
            if(nodesFound.size() == 0)
            {
                return entity;
            } 
            else if(nodesFound.size() > 1)
            {
                throw new EntityReaderException("Possibly corrupt data in Neo4J. Two nodes with the same ID found");
            }
            else
            {
                Node node = nodesFound.getSingle();
                entity = mapper.toEntity(node, m.getRelationNames(), m);                
            }
            
        }
        else
        {
            //TODO: Implement searching within manually created indexes
        }     
        
        return entity;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    public void close()
    {
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<Neo4JQuery> getQueryImplementor()
    {
        return null;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        log.debug("Persisting " + entity);
        Transaction tx = null;
        
        GraphDatabaseService graphDb = factory.getConnection();
        
        try
        {
            tx = graphDb.beginTx();
            
            //Top level node                        
            Node node = mapper.fromEntity(entity, rlHolders, graphDb, entityMetadata);          
            
            tx.success();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            tx.finish();
        }
        
    } 

}
