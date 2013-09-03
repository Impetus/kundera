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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * @author vivek.mishra Elastic search client implementation on {@link Client}
 * 
 */
public class ESClient extends ClientBase implements Client<ESQuery>, Batcher, ClientPropertiesSetter
{

    private ESClientFactory factory;

    private TransportClient txClient;

    private EntityReader reader;

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(ESClient.class);

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** batch size. */
    private int batchSize;

    private Map clientProperties;

    ESClient(final ESClientFactory factory, final TransportClient client, final Map<String, Object> externalProperties)
    {
        this.factory = factory;
        this.clientMetadata = factory.getClientMetadata();
        this.txClient = client;
        this.reader = new ESEntityReader();
        setBatchSize(getPersistenceUnit(), externalProperties);
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        try
        {
            Map<String, Object> values = new HashMap<String, Object>();

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

            String keyAsString = getKeyAsString(id, entityMetadata, metaModel);

            addSource(entity, values, entityType);

            addRelations(rlHolders, values);

            IndexResponse response = txClient
                    .prepareIndex(entityMetadata.getSchema().toLowerCase(),
                            entityMetadata.getEntityClazz().getSimpleName(), keyAsString).setSource(values).execute()
                    .actionGet();

            // IndexRequest request = new
            // IndexRequest(entityMetadata.getSchema().toLowerCase(),
            // entityMetadata.getEntityClazz().getSimpleName(), keyAsString);
            // txClient.prepareBulk().add(request)
            assert response.getId() != null;
        }
        finally
        {
            // Nothing as of now.
        }

    }

    private void addRelations(List<RelationHolder> rlHolders, Map<String, Object> values)
    {
        if (rlHolders != null)
        {
            for (RelationHolder relation : rlHolders)
            {
                values.put(relation.getRelationName(), relation.getRelationValue());
            }
        }
    }

    private void addSource(Object entity, Map<String, Object> values, EntityType entityType)
    {
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attrib : attributes)
        {
            if (!attrib.isAssociation())
            {
                Object value = PropertyAccessorHelper.getObject(entity, (Field) attrib.getJavaMember());
                values.put(((AbstractAttribute) attrib).getJPAColumnName(), value);
            }
        }
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        GetResponse get = null;

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        String keyAsString = getKeyAsString(key, metadata, metaModel);
        try
        {
            get = txClient
                    .prepareGet(metadata.getSchema().toLowerCase(), metadata.getEntityClazz().getSimpleName(),
                            keyAsString).setOperationThreaded(false).execute().get();
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

        Object result = null;

        if (get.isExists())
        {
            result = getInstance(entityClass, result);
            PropertyAccessorHelper.setId(result, metadata, key);
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
            log.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (IllegalAccessException iaex)
        {
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

        SearchResponse response = txClient.prepareSearch(entityMetadata.getSchema().toLowerCase()).setFilter(filter)
                .execute().actionGet();
        SearchHits hits = response.getHits();

        Object entity = null;

        for (SearchHit hit : hits.getHits())
        {
            entity = getInstance(clazz, entity);
            Map<String, Object> hitResult = hit.sourceAsMap();
            results.add(wrap(hitResult, entityType, entity));
        }

        return results;
    }

    private Object wrap(Map<String, Object> results, EntityType entityType, Object result)
    {

        Map<String, Object> relations = new HashMap<String, Object>();
        Object key = null;
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes)
        {

            String fieldName = ((AbstractAttribute) attribute).getJPAColumnName();

            // TODOOOO : Enum handling is needed.
            if (!attribute.isAssociation())
            {
                Object fieldValue = results.get(fieldName);

                key = onId(key, attribute, fieldValue);

                fieldValue = onEnum(attribute, fieldValue);

                // TODOO:This has to be corrected. Reason is, in case of execute
                // query over composite key. It will not work

                setField(result, key, attribute, fieldValue);
            }

            if (attribute.isAssociation())
            {
                Object fieldValue = results.get(fieldName);
                relations.put(fieldName, fieldValue);
            }
        }

        return relations.isEmpty() ? result : new EnhanceEntity(result, key, relations);
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

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(metadata.getEntityClazz());

            String keyAsString = getKeyAsString(pKey, metadata, metaModel);

            try
            {
                txClient.prepareDelete(metadata.getSchema().toLowerCase(), metadata.getEntityClazz().getSimpleName(),
                        keyAsString.toString()/* index, type, id */).setOperationThreaded(false).execute().get();
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
        // TODO find address by person id.
        GetResponse get = null;

        List results = new ArrayList();

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);

        try
        {
            SearchResponse response = txClient.prepareSearch(metadata.getSchema().toLowerCase())/*
                                                                                                 * .
                                                                                                 * addFields
                                                                                                 * (
                                                                                                 * "*"
                                                                                                 * )
                                                                                                 */
            .setTypes(entityClazz.getSimpleName()).setQuery(QueryBuilders.termQuery(colName, colValue)).execute().get();

            SearchHits hits = response.getHits();
            for (SearchHit hit : hits)
            {
                MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata()
                        .getMetamodel(metadata.getPersistenceUnit());

                EntityType entityType = metaModel.entity(entityClazz);
                Map<String, Object> searchResults = hit.getSource();
                // hit
                Object result = null;
                result = getInstance(entityClazz, result);
                result = wrap(searchResults, entityType, result);
                if (result != null)
                {
                    results.add(result);
                }
            }
        }
        catch (InterruptedException iex)
        {
            log.error("Error while find record of {}, Caused by :.", entityClazz.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (ExecutionException eex)
        {
            log.error("Error while find record of {}, Caused by :.", entityClazz.getSimpleName(), eex);
            throw new PersistenceException(eex);
        }

        return results;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<ESQuery> getQueryImplementor()
    {
        return ESQuery.class;
    }

    @Override
    public void addBatch(Node node)
    {

        if (node != null)
        {
            nodes.add(node);
        }

        onBatchLimit();

    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit()
    {
        if (batchSize > 0 && batchSize == nodes.size())
        {
            executeBatch();
            nodes.clear();
        }
    }

    @Override
    public int executeBatch()
    {
        BulkRequestBuilder bulkRequest = txClient.prepareBulk();

        try
        {
            for (Node node : nodes)
            {
                if (node.isDirty())
                {
                    node.handlePreEvent();
                    Object entity = node.getData();
                    Object id = node.getEntityId();
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());

                    MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata()
                            .getMetamodel(metadata.getPersistenceUnit());

                    EntityType entityType = metaModel.entity(metadata.getEntityClazz());

                    String key = getKeyAsString(id, metadata, metaModel);

                    if (node.isInState(RemovedState.class))
                    {
                        // create a delete request.

                        DeleteRequest request = new DeleteRequest(metadata.getSchema().toLowerCase(), metadata
                                .getEntityClazz().getSimpleName(), key);
                        bulkRequest.add(request);

                    }
                    else if (node.isUpdate())
                    {
                        Map<String, Object> values = new HashMap<String, Object>();
                        List<RelationHolder> relationHolders = getRelationHolders(node);

                        addSource(entity, values, entityType);

                        addRelations(relationHolders, values);

                        UpdateRequest request = new UpdateRequest(metadata.getSchema().toLowerCase(), metadata
                                .getEntityClazz().getSimpleName(), key).doc(values);
                        bulkRequest.add(request);
                    }
                    else
                    {
                        // create an insert request.
                        Map<String, Object> values = new HashMap<String, Object>();
                        List<RelationHolder> relationHolders = getRelationHolders(node);

                        addSource(entity, values, entityType);

                        addRelations(relationHolders, values);

                        IndexRequest request = new IndexRequest(metadata.getSchema().toLowerCase(), metadata
                                .getEntityClazz().getSimpleName(), key).source(values);
                        bulkRequest.add(request);

                    }

                }
            }

            BulkResponse response = null;
            if (nodes != null && !nodes.isEmpty())
            {
                // bulkRequest.setRefresh(true);
                response = bulkRequest.execute().actionGet();
            }
            return response != null ? response.getItems().length : 0;
        }
        finally
        {
            clear();
        }

    }

    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = null;
            nodes = new ArrayList<Node>();
        }
    }

    private String getKeyAsString(Object id, EntityMetadata metadata, MetamodelImpl metaModel)
    {
        if (metaModel.isEmbeddable(((AbstractAttribute) metadata.getIdAttribute()).getBindableJavaType()))
        {
            Object key = KunderaCoreUtils.prepareCompositeKey(metadata, metaModel, id);
            return key.toString();
        }

        return id.toString();

    }

    /**
     * @param persistenceUnit
     * @param puProperties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = null;
        if (puProperties != null)
        {
            Object externalBatchSize = /*
                                        * clientProperties != null ?
                                        * clientProperties
                                        * .get(PersistenceProperties
                                        * .KUNDERA_BATCH_SIZE) :
                                        */puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE);
            externalBatchSize = externalBatchSize != null ? externalBatchSize.toString() : null;
            batch_Size = puProperties != null ? (String) externalBatchSize : null;
            if (batch_Size != null)
            {
                batchSize = Integer.valueOf(batch_Size);
                if (batchSize == 0)
                {
                    throw new IllegalArgumentException("kundera.batch.size property must be numeric and > 0.");
                }
            }
        }
        else if (batch_Size == null)
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            batchSize = puMetadata != null ? puMetadata.getBatchSize() : 0;
        }
    }

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.clientProperties = properties;
    }

    private void setField(Object result, Object key, Attribute attribute, Object fieldValue)
    {
        if (key == null || !key.equals(fieldValue))
        {
            PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), fieldValue);
        }
    }

    private Object onId(Object key, Attribute attribute, Object fieldValue)
    {
        if (SingularAttribute.class.isAssignableFrom(attribute.getClass()) && ((SingularAttribute) attribute).isId())
        {
            key = fieldValue;
        }
        return key;
    }

    private Object onEnum(Attribute attribute, Object fieldValue)
    {
        if (((Field) attribute.getJavaMember()).getType().isEnum())
        {
            EnumAccessor accessor = new EnumAccessor();
            fieldValue = accessor.fromString(((AbstractAttribute) attribute).getBindableJavaType(),
                    fieldValue.toString());

        }
        return fieldValue;
    }

}
