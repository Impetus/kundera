/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author vivek.mishra Elastic search client implementation on {@link Client}
 * 
 */
public class ESClient extends ClientBase implements Client<ESQuery>
{

    private ESClientFactory factory;

    private TransportClient txClient;

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(ESClient.class);

    ESClient(final ESClientFactory factory, final TransportClient client)
    {
        this.factory = factory;
        this.txClient = client;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try
        {
            json = mapper.writeValueAsString(entity);
        }
        catch (JsonProcessingException jpex)
        {
            log.error("Error while persisting record of {}, Caused by :.", entityMetadata.getEntityClazz()
                    .getSimpleName(), jpex);
            throw new PersistenceException(jpex);
        }

        IndexResponse response = txClient
                .prepareIndex(entityMetadata.getSchema().toLowerCase(),
                        entityMetadata.getEntityClazz().getSimpleName(), id.toString()).setSource(json).execute()
                .actionGet();

        assert response.getId() != null;
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        // GetRequestBuilder getRequestBuilder =
        // txClient.prepareGet(metadata.getSchema().toLowerCase(),
        // metadata.getEntityClazz().getSimpleName(), key.toString());
        GetResponse get = null;
        try
        {
            get = txClient
                    .prepareGet(metadata.getSchema().toLowerCase(), metadata.getEntityClazz().getSimpleName(),
                            key.toString()).setOperationThreaded(false).execute().get();
        }
        catch (InterruptedException iex)
        {
            log.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (ExecutionException eex)
        {
            log.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), eex);
            throw new PersistenceException(eex);
        }

        Map<String, Object> results = get.getSource();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityClass);

        Object result = null;

        if (get.isExists())
        {
            result = getInstance(entityClass, result);
            result = wrap(results, entityType, result);
        }

        return result;
    }

    private Object getInstance(Class entityClass, Object result)
    {
        try
        {
            result = entityClass.newInstance();
        }
        catch (InstantiationException iex)
        {
            // TODO Auto-generated catch block
            log.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (IllegalAccessException iaex)
        {
            // TODO Auto-generated catch block
            log.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), iaex);
            throw new PersistenceException(iaex);
        }
        return result;
    }

    
    List executeQuery(FilterBuilder filter, final EntityMetadata entityMetadata)
    {

        Class clazz = entityMetadata.getEntityClazz();
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(clazz);

        List results = new ArrayList();
        
        SearchResponse response = txClient.prepareSearch(entityMetadata.getSchema().toLowerCase()).setFilter(filter).execute().actionGet();
        SearchHits hits = response.getHits();
        
        Object entity = null;

        for(SearchHit hit : hits.getHits())
        {
            entity = getInstance(clazz, entity);
            Map<String, Object> hitResult = hit.sourceAsMap();
            results.add(wrap(hitResult, entityType, entity));
        }
        
        return results;
    }
    
    private Object wrap(Map<String, Object> results, EntityType entityType,
            Object result)
    {

            Set<Attribute> attributes = entityType.getAttributes();
            for (Attribute attribute : attributes)
            {
                
                // TODOOOO : Enum handling is needed.
                if (!((Field) attribute.getJavaMember()).getType().isEnum())
                {
                    Object fieldValue = results.get(attribute.getName());
                    PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), fieldValue);
                }
            }

        return result;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        if (entity != null)
        {
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

            // metadata.getSchema().toLowerCase(),
            // metadata.getEntityClazz().getSimpleName(), key.toString())
            // .setOperationThreaded(false).execute().get()
            try
            {
                txClient.prepareDelete(metadata.getSchema().toLowerCase(), metadata.getEntityClazz().getSimpleName(),
                        pKey.toString()/* index, type, id */).setOperationThreaded(false).execute().get();
            }
            catch (InterruptedException iex)
            {
                log.error("Error while deleting record of {}, Caused by :.", pKey, iex);
                throw new PersistenceException(iex);
            }
            catch (ExecutionException eex)
            {
                log.error("Error while deleting record of {}, Caused by :.", pKey, eex);
                throw new PersistenceException(eex);
            }
        }

    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<ESQuery> getQueryImplementor()
    {
        return ESQuery.class;
    }

}
