package com.impetus.client.es.index;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.index.IndexerProperties;
import com.impetus.kundera.index.IndexerProperties.Node;
import com.impetus.kundera.index.IndexingException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.thoughtworks.xstream.XStream;

/**
 * @author vivek.mishra
 * 
 */
public class ESIndexer implements Indexer
{
    private Client client;

    /** The Constant UUID. */
    private static final long UUID = 6077004083174677888L;

    /** The Constant PARENT_ID_FIELD. */
    public static final String PARENT_ID_FIELD = UUID + ".parent.id";

    /** The Constant PARENT_ID_CLASS. */
    public static final String PARENT_ID_CLASS = UUID + ".parent.class";

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(ESIndexer.class);

    public ESIndexer()
    {
        init();
    }

    /**
     * @param client
     *            the client to set
     */
    public void setClient(Client client)
    {
        this.client = client;
    }

    @Override
    public void index(Class entityClazz, Map<String, Object> values, Object parentId, Class parentClazz)
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
            values.put("entity.class", metadata.getEntityClazz().getCanonicalName().toLowerCase());
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

    @Override
    public Map<String, Object> search(Class<?> clazz, String luceneQuery, int start, int count)
    {
        if (log.isInfoEnabled())
        {
            log.info("Executing lucene query " + luceneQuery);
        }

        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(clazz);

        ListenableActionFuture<SearchResponse> listenableActionFuture = client
                .prepareSearch(m.getSchema().toLowerCase()).setQuery(QueryBuilders.queryString(luceneQuery))
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

    @Override
    public void unIndex(Class entityClazz, Object key)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        DeleteResponse response = client
                .prepareDelete(metadata.getSchema().toLowerCase(), entityClazz.getSimpleName(), key.toString())
                .execute().actionGet();
    }

    @Override
    public void close()
    {
        if (client != null)
        {
            client.close();
        }
    }

    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, Class<?> childClass, Object entityId,
            int start, int count)
    {
        return search(parentClass, query, start, count);
    }

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
                    client = new TransportClient();
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
                        ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(properties
                                .getProperty("host"), Integer.parseInt(properties.getProperty("port"))));
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
}