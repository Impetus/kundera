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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.OrderByItem;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.es.utils.ESResponseWrapper;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class ESClient.
 * 
 * @author vivek.mishra Elastic search client implementation on {@link Client}
 */
public class ESClient extends ClientBase implements Client<ESQuery>, Batcher, ClientPropertiesSetter
{
    /** The factory. */
    private ESClientFactory factory;

    /** The tx client. */
    private TransportClient txClient;

    /** The reader. */
    private EntityReader reader;

    /** log for this class. */
    private static Logger logger = LoggerFactory.getLogger(ESClient.class);

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** batch size. */
    private int batchSize;

    /** The client properties. */
    private Map clientProperties;

    /** The set reresh indexes. */
    private boolean setRereshIndexes;

    /** The Constant KEY_SEPERATOR. */
    private static final String KEY_SEPERATOR = "\001";

    /** The es response reader. */
    private ESResponseWrapper esResponseReader = new ESResponseWrapper();

    /**
     * Instantiates a new ES client.
     * 
     * @param factory
     *            the factory
     * @param client
     *            the client
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     * @param persistenceUnit
     *            the persistence unit
     */
    ESClient(final ESClientFactory factory, final TransportClient client, final Map<String, Object> externalProperties,
            final KunderaMetadata kunderaMetadata, final String persistenceUnit)
    {
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.factory = factory;
        this.clientMetadata = factory.getClientMetadata();
        this.txClient = client;
        this.reader = new ESEntityReader(kunderaMetadata);
        setBatchSize(getPersistenceUnit(), externalProperties);
        setRefreshIndexes(
                kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit).getProperties(),
                externalProperties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.
     * metadata .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        try
        {
            Map<String, Object> values = new HashMap<String, Object>();

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(entityMetadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

            String keyAsString = getKeyAsString(id, entityMetadata, metaModel);

            addSource(entity, values, entityType);

            addRelations(rlHolders, values);
            addDiscriminator(values, entityType);

            IndexResponse response = txClient
                    .prepareIndex(entityMetadata.getSchema().toLowerCase(), entityMetadata.getTableName(), keyAsString)
                    .setSource(values).setRefresh(isRefreshIndexes()).execute().actionGet();

            assert response.getId() != null;
        }
        finally
        {
            // Nothing as of now.
        }

    }

    /**
     * Adds the discriminator.
     * 
     * @param values
     *            the values
     * @param entityType
     *            the entity type
     */
    private void addDiscriminator(Map<String, Object> values, EntityType entityType)
    {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            values.put(discrColumn, discrValue);
        }
    }

    /**
     * Adds the relations.
     * 
     * @param rlHolders
     *            the rl holders
     * @param values
     *            the values
     */
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

    /**
     * Adds the source.
     * 
     * @param entity
     *            the entity
     * @param values
     *            the values
     * @param entityType
     *            the entity type
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        GetResponse get = null;

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(metadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        String keyAsString = getKeyAsString(key, metadata, metaModel);
        try
        {
            get = txClient.prepareGet(metadata.getSchema().toLowerCase(), metadata.getTableName(), keyAsString)
                    .setOperationThreaded(false).execute().get();
        }
        catch (InterruptedException iex)
        {
            logger.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (ExecutionException eex)
        {
            logger.error("Error while find record of {}, Caused by :.", entityClass.getSimpleName(), eex);
            throw new PersistenceException(eex);
        }

        Map<String, Object> results = get.getSource();

        Object result = null;

        if (get.isExists())
        {
            result = KunderaCoreUtils.createNewInstance(entityClass);
            PropertyAccessorHelper.setId(result, metadata, key);
            result = esResponseReader.wrapFindResult(results, entityType, result, metadata, true);
        }

        return result;
    }

    /**
     * Execute query.
     *
     * @param filter
     *            the filter
     * @param aggregation
     *            the aggregation
     * @param entityMetadata
     *            the entity metadata
     * @param query
     *            the query
     * @param firstResult
     *            the first result
     * @param maxResults
     *            the max results
     * @return the list
     */
    public List executeQuery(QueryBuilder filter, AggregationBuilder aggregation, final EntityMetadata entityMetadata,
            KunderaQuery query, int firstResult, int maxResults)
    {
        String[] fieldsToSelect = query.getResult();
        Class clazz = entityMetadata.getEntityClazz();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());

        FilteredQueryBuilder queryBuilder = QueryBuilders.filteredQuery(null, filter);
        SearchRequestBuilder builder = txClient.prepareSearch(entityMetadata.getSchema().toLowerCase())
                .setTypes(entityMetadata.getTableName());

        addFieldsToBuilder(fieldsToSelect, clazz, metaModel, builder);

        if (aggregation == null)
        {
            builder.setQuery(queryBuilder);
            builder.setFrom(firstResult);
            builder.setSize(maxResults);
            addSortOrder(builder, query, entityMetadata);
        }
        else
        {
            logger.debug("Aggregated query identified");
            builder.addAggregation(aggregation);

            if (fieldsToSelect.length == 1
                    || (query.isSelectStatement() && query.getSelectStatement().hasGroupByClause()))
            {
                builder.setSize(0);
            }
        }

        SearchResponse response = null;
        logger.debug("Query generated: " + builder);

        try
        {
            response = builder.execute().actionGet();
            logger.debug("Query execution response: " + response);
        }
        catch (ElasticsearchException e)
        {
            logger.error("Exception occured while executing query on Elasticsearch.", e);
            throw new KunderaException("Exception occured while executing query on Elasticsearch.", e);
        }

        return esResponseReader.parseResponse(response, aggregation, fieldsToSelect, metaModel, clazz, entityMetadata,
                query);
    }

    /**
     * Adds the sort order.
     * 
     * @param builder
     *            the builder
     * @param query
     *            the query
     * @param entityMetadata
     *            the entity metadata
     */
    private void addSortOrder(SearchRequestBuilder builder, KunderaQuery query, EntityMetadata entityMetadata)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());

        List<OrderByItem> orderList = KunderaQueryUtils.getOrderByItems(query.getJpqlExpression());

        for (OrderByItem orderByItem : orderList)
        {
            String ordering = orderByItem.getOrdering().toString();

            if (ordering.equalsIgnoreCase(ESConstants.DEFAULT))
            {
                ordering = Expression.ASC;
            }

            builder.addSort(KunderaCoreUtils.getJPAColumnName(orderByItem.getExpression().toParsedText(),
                    entityMetadata, metaModel), SortOrder.valueOf(ordering));
        }
    }

    /**
     * Adds the fields to builder
     * 
     * @param fieldsToSelect
     *            the fields to select
     * @param clazz
     *            the clazz
     * @param metaModel
     *            the meta model
     * @param builder
     *            the builder
     */
    private void addFieldsToBuilder(String[] fieldsToSelect, Class clazz, MetamodelImpl metaModel,
            SearchRequestBuilder builder)
    {
        if (fieldsToSelect != null && fieldsToSelect.length > 1 && !(fieldsToSelect[1] == null))
        {
            logger.debug("Fields added in query are: ");
            for (int i = 1; i < fieldsToSelect.length; i++)
            {
                logger.debug(i + " : " + fieldsToSelect[i]);
                builder = builder.addField(((AbstractAttribute) metaModel.entity(clazz).getAttribute(fieldsToSelect[i]))
                        .getJPAColumnName());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        clear();
        reader = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        if (entity != null)
        {
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(metadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(metadata.getEntityClazz());

            String keyAsString = getKeyAsString(pKey, metadata, metaModel);

            try
            {
                txClient.prepareDelete(metadata.getSchema().toLowerCase(), metadata.getTableName(),
                        keyAsString.toString()/* index, type, id */).setRefresh(isRefreshIndexes()).execute().get();
            }
            catch (InterruptedException iex)
            {
                logger.error("Error while deleting record of {}, Caused by :.", pKey, iex);
                throw new PersistenceException(iex);
            }
            catch (ExecutionException eex)
            {
                logger.error("Error while deleting record of {}, Caused by :.", pKey, eex);
                throw new PersistenceException(eex);
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String tableName = joinTableData.getJoinTableName();

        String inverseJoinColumn = joinTableData.getInverseJoinColumnName();

        String joinColumn = joinTableData.getJoinColumnName();

        String schemaName = joinTableData.getSchemaName();

        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        Set<Object> joinKeys = joinTableRecords.keySet();

        BulkRequestBuilder bulkRequest = txClient.prepareBulk().setRefresh(isRefreshIndexes());

        /**
         * 1_p => 1_a1,1_a2 1_a1=> 1_p,1_p1
         * 
         * Example: join table : PERSON_ADDRESS join column : PERSON_ID (1_p)
         * inverse join column : ADDRESS_ID (1_a) store in ES: schema name:
         * PERSON_ADDRESS type: PERSON id: 1_p\0011_a
         * 
         * PERSON_ADDRESS:1_p_1_a PERSON_ID 1_p ADDRESS_ID 1_a
         * 
         * source: (PERSON_ID, 1_p) (ADDRESS_ID, 1_a)
         * 
         * embeddable keys over many to many does not work.
         */

        boolean found = false;
        for (Object key : joinKeys)
        {
            Set<Object> inversejoinTableRecords = joinTableRecords.get(key);
            Map<String, Object> source = new HashMap<String, Object>();

            for (Object inverseObj : inversejoinTableRecords)
            {
                source = new HashMap<String, Object>();
                source.put(joinTableData.getJoinColumnName(), key);
                source.put(joinTableData.getInverseJoinColumnName(), inverseObj);

                String joinKeyAsStr = PropertyAccessorHelper.getString(key);
                String inverseKeyAsStr = PropertyAccessorHelper.getString(inverseObj);

                String keyAsString = joinKeyAsStr + KEY_SEPERATOR + inverseKeyAsStr;
                IndexRequest request = new IndexRequest(schemaName.toLowerCase(), tableName, keyAsString)
                        .source(source);
                found = true;
                bulkRequest.add(request);
            }
        }

        // check made, as bulk request throws an error, in case no request is
        // present.
        if (found)
        {
            bulkRequest.execute().actionGet();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // fetch list ADDRESS_ID for given PERSON_ID
        QueryBuilder filterBuilder = new TermQueryBuilder(pKeyColumnName, pKeyColumnValue);

        SearchResponse response = txClient.prepareSearch(schemaName.toLowerCase()).setTypes(tableName)
                .setPostFilter(filterBuilder).addField(columnName).execute().actionGet();

        SearchHits hits = response.getHits();

        List columns = new ArrayList();
        for (SearchHit hit : hits.getHits())
        {
            Map<String, SearchHitField> fields = hit.getFields();
            columns.add(fields.get(columnName).getValue());
        }

        return columns;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {

        TermQueryBuilder filter = QueryBuilders.termQuery(columnName, columnValue);

        SearchResponse response = txClient.prepareSearch(schemaName.toLowerCase()).setTypes(tableName)
                .addField(pKeyName).setPostFilter(filter).execute().actionGet();

        SearchHits hits = response.getHits();

        Long length = hits.getTotalHits();
        int absoluteLength = length.intValue();
        Object[] ids = new Object[absoluteLength];

        int counter = 0;
        for (SearchHit hit : hits.getHits())
        {
            Map<String, SearchHitField> fields = hit.getFields();
            ids[counter++] = fields.get(pKeyName).getValue();
        }

        return ids;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO: implement using scroll/scan and bulk delete requests
        // Map<String, Object> querySource = new HashMap<String, Object>();
        // querySource.put(columnName, columnValue);
        //
        // DeleteByQueryRequestBuilder deleteQueryBuilder =
        // txClient.prepareDeleteByQuery(schemaName.toLowerCase())
        // .setSource(querySource).setTypes(tableName);
        //
        // deleteQueryBuilder.execute().actionGet();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        GetResponse get = null;

        List results = new ArrayList();

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        try
        {
            SearchResponse response = txClient
                    .prepareSearch(metadata.getSchema()
                            .toLowerCase())/*
                                            * . addFields ( "*" )
                                            */
                    .setTypes(metadata.getTableName()).setQuery(QueryBuilders.termQuery(colName, colValue)).execute()
                    .get();

            SearchHits hits = response.getHits();
            for (SearchHit hit : hits)
            {
                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                        .getMetamodel(metadata.getPersistenceUnit());

                EntityType entityType = metaModel.entity(entityClazz);
                Map<String, Object> searchResults = hit.getSource();
                // hit
                Object result = null;
                result = KunderaCoreUtils.createNewInstance(entityClazz);
                result = esResponseReader.wrapFindResult(searchResults, entityType, result, metadata, false);
                if (result != null)
                {
                    results.add(result);
                }
            }
        }
        catch (InterruptedException iex)
        {
            logger.error("Error while find record of {}, Caused by :.", entityClazz.getSimpleName(), iex);
            throw new PersistenceException(iex);
        }
        catch (ExecutionException eex)
        {
            logger.error("Error while find record of {}, Caused by :.", entityClazz.getSimpleName(), eex);
            throw new PersistenceException(eex);
        }

        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<ESQuery> getQueryImplementor()
    {
        return ESQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera
     * .graph.Node)
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {
        BulkRequestBuilder bulkRequest = txClient.prepareBulk().setRefresh(isRefreshIndexes());

        try
        {
            for (Node node : nodes)
            {
                if (node.isDirty())
                {
                    node.handlePreEvent();
                    Object entity = node.getData();
                    Object id = node.getEntityId();
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            node.getDataClass());

                    MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                            .getMetamodel(metadata.getPersistenceUnit());

                    EntityType entityType = metaModel.entity(metadata.getEntityClazz());

                    String key = getKeyAsString(id, metadata, metaModel);

                    if (node.isInState(RemovedState.class))
                    {
                        // create a delete request.

                        DeleteRequest request = new DeleteRequest(metadata.getSchema().toLowerCase(),
                                metadata.getTableName(), key);
                        bulkRequest.add(request);

                    }
                    else if (node.isUpdate())
                    {
                        Map<String, Object> values = new HashMap<String, Object>();
                        List<RelationHolder> relationHolders = getRelationHolders(node);

                        addSource(entity, values, entityType);

                        addRelations(relationHolders, values);

                        UpdateRequest request = new UpdateRequest(metadata.getSchema().toLowerCase(),
                                metadata.getTableName(), key).doc(values);
                        bulkRequest.add(request);
                    }
                    else
                    {
                        // create an insert request.
                        Map<String, Object> values = new HashMap<String, Object>();
                        List<RelationHolder> relationHolders = getRelationHolders(node);

                        addSource(entity, values, entityType);

                        addRelations(relationHolders, values);

                        IndexRequest request = new IndexRequest(metadata.getSchema().toLowerCase(),
                                metadata.getTableName(), key).source(values);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#clear()
     */
    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = new ArrayList<Node>();
        }
    }

    /**
     * Gets the key as string.
     * 
     * @param id
     *            the id
     * @param metadata
     *            the metadata
     * @param metaModel
     *            the meta model
     * @return the key as string
     */
    private String getKeyAsString(Object id, EntityMetadata metadata, MetamodelImpl metaModel)
    {
        if (metaModel.isEmbeddable(((AbstractAttribute) metadata.getIdAttribute()).getBindableJavaType()))
        {
            return KunderaCoreUtils.prepareCompositeKey(metadata, id);
        }
        return id.toString();
    }

    /**
     * Sets the batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = null;
        if (puProperties != null)
        {
            Object externalBatchSize = puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE);
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
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            batchSize = puMetadata != null ? puMetadata.getBatchSize() : 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientPropertiesSetter#
     * populateClientProperties (com.impetus.kundera.client.Client,
     * java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.clientProperties = properties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(EsIdGenerator.class);
    }

    /**
     * Sets the refresh indexes.
     *
     * @param puProps
     *            the pu props
     * @param externalProperties
     *            the external properties
     */
    private void setRefreshIndexes(Properties puProps, Map<String, Object> externalProperties)
    {
        Object refreshIndexes = null;

        /*
         * Check from properties set while creating emf
         * 
         */
        if (externalProperties.get(ESConstants.KUNDERA_ES_REFRESH_INDEXES) != null)
        {

            refreshIndexes = externalProperties.get(ESConstants.KUNDERA_ES_REFRESH_INDEXES);

        }

        /*
         * Check from PU Properties
         * 
         */
        if (refreshIndexes == null && puProps.get(ESConstants.KUNDERA_ES_REFRESH_INDEXES) != null)
        {

            refreshIndexes = puProps.get(ESConstants.KUNDERA_ES_REFRESH_INDEXES);

        }

        if (refreshIndexes != null)
        {
            if (refreshIndexes instanceof Boolean)
            {
                this.setRereshIndexes = (boolean) refreshIndexes;
            }
            else
            {
                this.setRereshIndexes = Boolean.parseBoolean((String) refreshIndexes);
            }
        }
    }

    /**
     * Checks if is refresh indexes.
     *
     * @return true, if is refresh indexes
     */
    private boolean isRefreshIndexes()
    {

        if (clientProperties != null)
        {

            Object refreshIndexes = clientProperties.get(ESConstants.ES_REFRESH_INDEXES);

            if (refreshIndexes != null && refreshIndexes instanceof Boolean)
            {
                return (boolean) refreshIndexes;
            }
            else
            {
                return Boolean.parseBoolean((String) refreshIndexes);
            }

        }
        else
        {
            return this.setRereshIndexes;
        }
    }
}