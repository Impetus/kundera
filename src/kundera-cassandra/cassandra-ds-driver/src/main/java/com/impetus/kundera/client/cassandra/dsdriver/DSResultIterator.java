package com.impetus.kundera.client.cassandra.dsdriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EntityType;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.impetus.client.cassandra.query.ResultIterator;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.Query;

/**
 * The Class DSResultIterator.
 * 
 * @author karthikp.manchala
 * 
 * @param <E>
 *            the element type
 */
public class DSResultIterator<E> extends ResultIterator<E>
{

    /** The r set. */
    private ResultSet rSet;

    /** The row iter. */
    private Iterator<Row> rowIter;

    /**
     * Constructor with parameters.
     * 
     * @param query
     *            the query
     * @param m
     *            the m
     * @param client
     *            the client
     * @param reader
     *            the reader
     * @param fetchSize
     *            the fetch size
     * @param kunderaMetadata
     *            the kundera metadata
     */
    DSResultIterator(final Query query, final EntityMetadata m, final Client client, final EntityReader reader,
            final int fetchSize, final KunderaMetadata kunderaMetadata)
    {
        super((javax.persistence.Query) query, m, client, reader, fetchSize, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.query.ResultIterator#hasNext()
     */
    @Override
    public boolean hasNext()
    {
        if (fetchSize != 0 && (count % fetchSize) == 0)
        {
            try
            {
                results = populateEntities(entityMetadata, client);
                count = 0;
            }
            catch (Exception e)
            {
                throw new PersistenceException("Error while scrolling over results, Caused by :.", e);
            }
        }
        if (results != null && !results.isEmpty() && count < results.size())
        {
            return true;
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.query.ResultIterator#next()
     */
    @Override
    public E next()
    {
        if (results != null && !results.isEmpty() && count < results.size())
        {
            current = results.get(count++);
            return current;
        }
        else
        {
            throw new NoSuchElementException(
                    "No object found in the iterator... Use hasNext() to check for valid next()");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.cassandra.query.ResultIterator#populateEntities(com
     * .impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<E> populateEntities(EntityMetadata m, Client client) throws Exception
    {
        int count = 0;
        List results = new ArrayList();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Map<String, Object> relationalValues = new HashMap<String, Object>();
        if (rSet == null)
        {
            String parsedQuery = query.onQueryOverCQL3(m, client, metaModel, null);
            Statement statement = new SimpleStatement(parsedQuery);
            statement.setFetchSize(fetchSize);
            rSet = ((DSClient) client).executeStatement(statement);
            rowIter = rSet.iterator();
        }
        while (rowIter.hasNext() && count++ < fetchSize)
        {
            Object entity = null;
            Row row = rowIter.next();
            ((DSClient) client).populateObjectFromRow(entityMetadata, metaModel, entityType, results, relationalValues,
                    entity, row);
        }
        return results;
    }

}
