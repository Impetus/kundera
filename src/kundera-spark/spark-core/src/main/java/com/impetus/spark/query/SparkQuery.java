/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.spark.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.DataFrame;
import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.Query;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.spark.client.SparkClient;
import com.impetus.spark.client.SparkDataClient;
import com.impetus.spark.client.SparkDataClientFactory;
import com.impetus.spark.constants.SparkPropertiesConstants;

/**
 * The Class SparkQuery.
 * 
 * @author: karthikp.manchala
 */
public class SparkQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Logger logger = LoggerFactory.getLogger(SparkQuery.class);

    /**
     * Instantiates a new spark query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public SparkQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        try
        {
            String query = appMetadata.getQuery(getJPAQuery());
            if (kunderaQuery.isNative())
            {
                // Native Query Support is enabled
                kunderaQuery.setAggregated(isAggregatedQuery());
                return ((SparkClient) client).executeQuery(query == null ? getJPAQuery() : query, m, kunderaQuery);
            }
        }
        catch (Exception e)
        {

            logger.error("Error during executing query, Caused by:", e);
            throw new QueryHandlerException(e);
        }
        return null;
    }

    /**
     * Parses the insert into query.
     * 
     * @param query
     *            the query
     * @return the map
     */
    private Map<String, Object> parseInsertIntoQuery(String query)
    {
        Map<String, Object> persistDetails = new HashMap<String, Object>();
        String insertReg = "(?i)^insert\\s+into\\s+(\\S+)\\s+(?:as\\s+(\\S+)\\s+)?FROM\\s+\\((.*)\\)$";
        Pattern r = Pattern.compile(insertReg);

        Matcher m = r.matcher(query);
        if (m.find())
        {
            try
            {
                parsePersistClause(m.group(1), persistDetails);
                persistDetails.put("format", m.group(2));
                persistDetails.put("fetchQuery", m.group(3));
            }
            catch (Exception e)
            {
                throw new KunderaException("Invalid Query");
            }
        }
        else
        {
            throw new KunderaException("Invalid Query");
        }

        return persistDetails;
    }

    /**
     * Parses the persist clause.
     * 
     * @param persistClause
     *            the persist clause
     * @param persistDetails
     *            the persist details
     * @return the map
     * @throws KunderaException
     *             the kundera exception
     */
    private Map<String, Object> parsePersistClause(String persistClause, Map<String, Object> persistDetails)
            throws KunderaException
    {
        Pattern pattern = Pattern.compile("^([^.]+)\\.(?:([^.]+)\\.([^.]+)|\\[([^\\]]+)\\])$");
        Matcher matcher = pattern.matcher(persistClause);

        if (matcher.find())
        {

            persistDetails.put("client", matcher.group(1));

            switch (matcher.group(1).toLowerCase())
            {

            case SparkPropertiesConstants.CLIENT_CASSANDRA:
                persistDetails.put("keyspace", matcher.group(2));
                persistDetails.put("table", matcher.group(3));
                break;

            case SparkPropertiesConstants.CLIENT_FS:
                persistDetails.put(SparkPropertiesConstants.FS_OUTPUT_FILE_PATH, matcher.group(4));
                break;

            case SparkPropertiesConstants.CLIENT_HDFS:
                persistDetails.put(SparkPropertiesConstants.HDFS_OUTPUT_FILE_PATH, matcher.group(4));
                break;
            case SparkPropertiesConstants.CLIENT_HIVE:
                persistDetails.put("keyspace", matcher.group(2));
                persistDetails.put("table", matcher.group(3));
                break;

            default:
                throw new UnsupportedOperationException("Not Supported for this client");
            }
        }
        else
        {
            throw new KunderaException("Invalid Query");
        }

        return persistDetails;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#findUsingLucene(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        throw new KunderaException("Query on entities having relations is currently not supported in kundera-spark.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        Map<String, Object> persistDetails = new HashMap<String, Object>();
        DataFrame dataFrame;
        try
        {
            String query = getJPAQuery();// .toLowerCase();

            persistDetails = parseInsertIntoQuery(query);

            dataFrame = getDataFrameToPersist(query, (String) persistDetails.get("fetchQuery"));

            String clientName = (String) persistDetails.get("client");

            SparkDataClient dataClient = SparkDataClientFactory.getDataClient(clientName);
            dataClient.saveDataFrame(dataFrame, getEntityMetadata().getEntityClazz(), persistDetails);
        }
        catch (Exception e)
        {
            logger.error("Error during executing query, Caused by:", e);
            throw new QueryHandlerException(e);
        }

        return getDataFrameSize(dataFrame);
    }

    /**
     * Gets the data frame size.
     * 
     * @param dataFrame
     *            the data frame
     * @return the data frame size
     */
    public int getDataFrameSize(DataFrame dataFrame)
    {
        long l = dataFrame != null ? dataFrame.count() : 0;
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)
        {
            logger.error(l + " cannot be cast to int without changing its value.");
            return 0;
        }
        return (int) l;
    }

    /**
     * Gets the data frame to persist.
     * 
     * @param query
     *            the query
     * @param subQuery
     *            the sub query
     * @return the data frame to persist
     */
    private DataFrame getDataFrameToPersist(String query, String subQuery)
    {
        EntityMetadata entityMetadata = getEntityMetadata();

        Client client = entityMetadata != null ? persistenceDelegeator.getClient(entityMetadata)
                : persistenceDelegeator.getClient(kunderaQuery.getPersistenceUnit());

        return ((SparkClient) client).getDataFrame(subQuery, entityMetadata, getKunderaQuery());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate()
    {
        return null;
    }

    /**
     * Checks if is aggregated query.
     * 
     * @return true, if is aggregated query
     */
    boolean isAggregatedQuery()
    {
        if (kunderaQuery.getSelectStatement() != null)
        {
            Expression exp = ((SelectClause) kunderaQuery.getSelectStatement().getSelectClause()).getSelectExpression();
            return AggregateFunction.class.isAssignableFrom(exp.getClass());
        }
        else
        {
            return false;
        }
    }

}
