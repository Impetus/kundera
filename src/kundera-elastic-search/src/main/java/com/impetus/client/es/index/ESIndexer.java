package com.impetus.client.es.index;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.OrderByItem;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.ListIterable;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.impetus.client.es.ESConstants;
import com.impetus.client.es.ESQuery;
import com.impetus.client.es.utils.ESResponseWrapper;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.index.IndexerProperties;
import com.impetus.kundera.index.IndexerProperties.Node;
import com.impetus.kundera.index.IndexingException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.thoughtworks.xstream.XStream;

/**
 * The Class ESIndexer.
 * 
 * @author vivek.mishra
 */
public class ESIndexer implements Indexer
{

    /** The client. */
    private Client client;

    /** The Constant UUID. */
    private static final long UUID = 6077004083174677888L;

    /** The Constant PARENT_ID_FIELD. */
    // ES 2.0+ doesn't support '.' in field name (Causes MapperParsingException)
    public static final String PARENT_ID_FIELD = UUID + "_parent_id";

    /** The Constant PARENT_ID_CLASS. */
    public static final String PARENT_ID_CLASS = UUID + "_parent_class";

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(ESIndexer.class);

    /**
     * Instantiates a new ES indexer.
     */
    public ESIndexer()
    {
        init();
    }

    /**
     * Sets the client.
     * 
     * @param client
     *            the client to set
     */
    public void setClient(Client client)
    {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#index(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.util.Map,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public void index(Class entityClazz, EntityMetadata metadata, Map<String, Object> values, Object parentId,
            Class parentClazz)
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            values.put("entity_class", metadata.getEntityClazz().getCanonicalName().toLowerCase());
            if (parentId != null)
            {
                values.put(PARENT_ID_FIELD, parentId);
            }
            if (entityClazz != null)
            {
                values.put(PARENT_ID_CLASS, parentClazz.getCanonicalName().toLowerCase());
            }
            String json = mapper.writeValueAsString(values);
            String idColumnName = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
            Object id = PropertyAccessorHelper.fromSourceToTargetClass(String.class,
                    ((AbstractAttribute) metadata.getIdAttribute()).getBindableJavaType(), values.get(idColumnName));

            ListenableActionFuture<IndexResponse> listenableActionFuture = client
                    .prepareIndex(metadata.getSchema().toLowerCase(), entityClazz.getSimpleName(), id.toString())
                    .setSource(json).execute();
            IndexResponse response = listenableActionFuture.actionGet();
        }
        catch (JsonGenerationException e)
        {
            log.error("Error while creating json document", e);
            throw new KunderaException(e);
        }
        catch (JsonMappingException e)
        {
            log.error("Error while creating json document", e);
            throw new KunderaException(e);
        }
        catch (IOException e)
        {
            log.error("Error while creating json document", e);
            throw new KunderaException(e);
        }
        catch (Exception e)
        {
            log.error("Error while creating indexes on document", e);
            throw new KunderaException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String, int,
     * int)
     */
    @Override
    public Map<String, Object> search(Class<?> clazz, EntityMetadata m, String luceneQuery, int start, int count)
    {
        if (log.isInfoEnabled())
        {
            log.info("Executing lucene query " + luceneQuery);
        }

        ListenableActionFuture<SearchResponse> listenableActionFuture = client
                .prepareSearch(m.getSchema().toLowerCase()).setQuery(QueryBuilders.queryStringQuery(luceneQuery))
                .setSize(40000).execute();
        SearchResponse response = listenableActionFuture.actionGet();

        Map<String, Object> results = new HashMap<String, Object>();
        for (SearchHit hit : response.getHits())
        {
            Object id = PropertyAccessorHelper.fromSourceToTargetClass(
                    ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType(), String.class, hit.getId());
            results.put(hit.getId(), id);
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.index.Indexer#search(com.impetus.kundera.persistence
     * .EntityManagerFactoryImpl.KunderaMetadata,
     * com.impetus.kundera.query.KunderaQuery,
     * com.impetus.kundera.persistence.PersistenceDelegator,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public Map<String, Object> search(KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery,
            PersistenceDelegator persistenceDelegator, EntityMetadata m, int firstResult, int maxResults)
    {
        ESQuery query = new ESQuery<>(kunderaQuery, persistenceDelegator, kunderaMetadata);
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(m.getPersistenceUnit());
        Expression whereExpression = KunderaQueryUtils.getWhereClause(kunderaQuery.getJpqlExpression());

        QueryBuilder filter = whereExpression != null ? query.getEsFilterBuilder()
                .populateFilterBuilder(((WhereClause) whereExpression).getConditionalExpression(), m) : null;

        FilteredQueryBuilder queryBuilder = QueryBuilders.filteredQuery(null, filter);
        SearchResponse response = getSearchResponse(kunderaQuery, queryBuilder, filter, query, m, firstResult,
                maxResults, kunderaMetadata);

        return buildResultMap(response, kunderaQuery, m, metaModel);
    }

    /**
     * Builds the result map.
     * 
     * @param esResponseReader
     *            the es response reader
     * @param response
     *            the response
     * @param query
     *            the query
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @return the map
     */
    private Map<String, Object> buildResultMap(SearchResponse response, KunderaQuery query, EntityMetadata m,
            MetamodelImpl metaModel)
    {
        Map<String, Object> map = new LinkedHashMap<>();
        ESResponseWrapper esResponseReader = new ESResponseWrapper();

        for (SearchHit hit : response.getHits())
        {
            Object id = PropertyAccessorHelper.fromSourceToTargetClass(
                    ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType(), String.class, hit.getId());
            map.put(hit.getId(), id);
        }

        Map<String, Object> aggMap = esResponseReader.parseAggregations(response, query, metaModel, m.getEntityClazz(),
                m);
        ListIterable<Expression> selectExpressionOrder = esResponseReader.getSelectExpressionOrder(query);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(Constants.AGGREGATIONS, aggMap);
        resultMap.put(Constants.PRIMARY_KEYS, map);
        resultMap.put(Constants.SELECT_EXPRESSION_ORDER, selectExpressionOrder);

        return resultMap;
    }

    /**
     * Gets the search response.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param queryBuilder
     *            the query builder
     * @param filter
     *            the filter
     * @param query
     *            the query
     * @param m
     *            the m
     * @param kunderaMetadata
     * @return the search response
     */
    private SearchResponse getSearchResponse(KunderaQuery kunderaQuery, FilteredQueryBuilder queryBuilder,
            QueryBuilder filter, ESQuery query, EntityMetadata m, int firstResult, int maxResults,
            KunderaMetadata kunderaMetadata)
    {
        SearchRequestBuilder builder = client.prepareSearch(m.getSchema().toLowerCase())
                .setTypes(m.getEntityClazz().getSimpleName());
        AggregationBuilder aggregation = query.buildAggregation(kunderaQuery, m, filter);

        if (aggregation == null)
        {
            builder.setQuery(queryBuilder);
            builder.setFrom(firstResult);
            builder.setSize(maxResults);
            addSortOrder(builder, kunderaQuery, m, kunderaMetadata);
        }
        else
        {
            log.debug("Aggregated query identified");
            builder.addAggregation(aggregation);

            if (kunderaQuery.getResult().length == 1 || (kunderaQuery.isSelectStatement()
                    && KunderaQueryUtils.hasGroupBy(kunderaQuery.getJpqlExpression())))
            {
                builder.setSize(0);
            }
        }

        SearchResponse response = null;
        log.debug("Query generated: " + builder);

        try
        {
            response = builder.execute().actionGet();
            log.debug("Query execution response: " + response);
        }
        catch (ElasticsearchException e)
        {
            log.error("Exception occured while executing query on Elasticsearch.", e);
            throw new KunderaException("Exception occured while executing query on Elasticsearch.", e);
        }

        return response;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#unIndex(java.lang.Class,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.metadata.model.MetamodelImpl)
     */
    @Override
    public void unIndex(Class entityClazz, Object entity, EntityMetadata metadata, MetamodelImpl metamodelImpl)
    {
        Object id = PropertyAccessorHelper.getId(entity, metadata);
        DeleteResponse response = client
                .prepareDelete(metadata.getSchema().toLowerCase(), entityClazz.getSimpleName(), id.toString()).execute()
                .actionGet();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#close()
     */
    @Override
    public void close()
    {
        if (client != null)
        {
            client.close();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.String,
     * java.lang.Class, com.impetus.kundera.metadata.model.EntityMetadata,
     * java.lang.Class, com.impetus.kundera.metadata.model.EntityMetadata,
     * java.lang.Object, int, int)
     */
    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, EntityMetadata parentMetadata,
            Class<?> childClass, EntityMetadata childMetadata, Object entityId, int start, int count)
    {
        return search(parentClass, parentMetadata, query, start, count);
    }

    /**
     * Inits the.
     */
    public void init()
    {

        InputStream inStream = this.getClass().getClassLoader().getResourceAsStream("esindexer.xml");
        XStream xStream = getXStreamObject();

        if (inStream != null)
        {
            Object o = xStream.fromXML(inStream);
            IndexerProperties indexerProperties = (IndexerProperties) o;
            List<Node> nodes = indexerProperties.getNodes();

            if (nodes == null || (nodes != null && nodes.isEmpty()))
            {
                log.error("Nodes should not be empty/null");
                throw new IndexingException("Nodes should not be empty/null");
            }
            else
            {
                if (client == null)
                {
                    client = TransportClient.builder().build();
                }
                for (Node node : nodes)
                {
                    Properties properties = node.getProperties();
                    if (properties != null)
                    {
                        if (properties.getProperty("host") == null
                                || !StringUtils.isNumeric(properties.getProperty("port"))
                                || properties.getProperty("port").isEmpty())
                        {
                            log.error("Host or port should not be null / port should be numeric");
                            throw new IllegalArgumentException(
                                    "Host or port should not be null / port should be numeric");
                        }
                        ((TransportClient) client).addTransportAddress(
                                new InetSocketTransportAddress(new InetSocketAddress(properties.getProperty("host"),
                                        Integer.parseInt(properties.getProperty("port")))));
                    }
                }
            }
        }
        else
        {
            log.error("No indexer setting provided, please provide valid indexer settings in esindexer.xml");
            throw new IndexingException(
                    "No indexer setting provided, please provide valid indexer settings in esindexer.xml");
        }
    }

    /**
     * get XStream Object.
     * 
     * @return XStream object.
     */
    private XStream getXStreamObject()
    {
        XStream stream = new XStream();
        stream.alias("indexerProperties", IndexerProperties.class);
        stream.alias("node", IndexerProperties.Node.class);
        return stream;
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
     * @param kunderaMetadata
     */
    private void addSortOrder(SearchRequestBuilder builder, KunderaQuery query, EntityMetadata entityMetadata,
            KunderaMetadata kunderaMetadata)
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
}