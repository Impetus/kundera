/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.cassandra.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * The Class CassandraEntityReader.
 * 
 * @author vivek.mishra
 */
public class CassandraEntityReader extends AbstractEntityReader implements EntityReader
{
    
    /** The Constant MIN_. */
    private static final String MIN_ = "min";

    /** The Constant MAX_. */
    private static final String MAX_ = "max";

    /** The conditions. */
    private Map<Boolean, List<IndexClause>> conditions = new HashMap<Boolean, List<IndexClause>>();

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(CassandraEntityReader.class);

    /**
     * Instantiates a new cassandra entity reader.
     *
     * @param kunderaQuery the kundera query
     * @param kunderaMetadata the kundera metadata
     */
    public CassandraEntityReader(KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        this.kunderaQuery = kunderaQuery;
    }

    /**
     * Instantiates a new cassandra entity reader.
     *
     * @param kunderaMetadata the kundera metadata
     */
    public CassandraEntityReader(final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.AbstractEntityReader#findById(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        return super.findById(primaryKey, m, client);
    }

    /**
     * Method responsible for reading back entity and relations using secondary
     * indexes(if it holds any relation), else retrieve row keys using lucene.
     *
     * @param m            entity meta data
     * @param client            client instance
     * @param maxResults the max results
     * @return list of wrapped enhance entities.
     */

    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults)
    {
        if (log.isInfoEnabled())
        {
            log.info("On populate relation via JPQL");
        }
        List<EnhanceEntity> ls = null;
        List<String> relationNames = m.getRelationNames();
        boolean isParent = m.isParent();

        boolean isRowKeyQuery = conditions != null ? conditions.keySet().iterator().next() : false;

        // If Query is not for find by range.
        if (!isRowKeyQuery)
        {
            // If holding associations.
            if (!isParent)
            {
                // In case need to use secondary indexes.
                if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
                {
                    ls = ((CassandraClientBase) client).find(m, relationNames, this.conditions.get(isRowKeyQuery),
                            maxResults, null);
                }
                else
                {
                    // prepare lucene query and find.
                    Set<String> rSet = fetchDataFromLucene(m.getEntityClazz(), client);

                    try
                    {
                        ls = (List<EnhanceEntity>) ((CassandraClientBase) client).find(m.getEntityClazz(),
                                relationNames, true, m, rSet.toArray(new Object[] {}));
                    }
                    catch (Exception e)
                    {
                        log.error("Error while executing handleAssociation for cassandra, Caused by: ", e);
                        throw new QueryHandlerException(e);
                    }
                }
            }
            else
            {
                if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
                {
                    // in case need to search on secondry columns and it is not
                    // set
                    // to true!
                    ls = ((CassandraClientBase) client).find(this.conditions.get(isRowKeyQuery), m, true,
                            m.getRelationNames(), maxResults, null);
                }
                else
                {
                    ls = onAssociationUsingLucene(m, client, ls);
                }
            }
        }
        else
        {
            ls = handleFindByRange(m, client, ls, conditions, isRowKeyQuery, null, maxResults);
        }
        return ls;
    }

    /**
     * Handle find by range.
     *
     * @param m            the m
     * @param client            the client
     * @param result            the result
     * @param ixClause            the ix clause
     * @param isRowKeyQuery            the is row key query
     * @param columns the columns
     * @param maxResults the max results
     * @return the list
     */
    public List handleFindByRange(EntityMetadata m, Client client, List result,
            Map<Boolean, List<IndexClause>> ixClause, boolean isRowKeyQuery, List<String> columns, int maxResults)
    {
        List<IndexExpression> expressions = ixClause.get(isRowKeyQuery).get(0).getExpressions();

        if (expressions == null)
        {
            return null;
        }

        Map<String, byte[]> rowKeys = getRowKeyValue(expressions,
                ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName());

        byte[] minValue = rowKeys.get(MIN_);
        byte[] maxVal = rowKeys.get(MAX_);

        try
        {
            result = ((CassandraClientBase) client).findByRange(minValue, maxVal, m, m.getRelationNames() != null
                    && !m.getRelationNames().isEmpty(), m.getRelationNames(), columns, expressions, maxResults);
        }
        catch (Exception e)
        {
            log.error("Error while executing find by range, Caused by: ", e);
            throw new QueryHandlerException(e);
        }
        return result;
    }

    /**
     * Read from index table.
     *
     * @param m the m
     * @param client the client
     * @param indexClauseMap the index clause map
     * @return the list
     */
    public List<EnhanceEntity> readFromIndexTable(EntityMetadata m, Client client,
            Map<Boolean, List<IndexClause>> indexClauseMap)
    {
        List<SearchResult> searchResults = new ArrayList<SearchResult>();
        List<Object> primaryKeys = new ArrayList<Object>();

        String columnFamilyName = m.getTableName() + Constants.INDEX_TABLE_SUFFIX;
        searchResults = ((CassandraClientBase) client).searchInInvertedIndex(columnFamilyName, m, indexClauseMap);

        Map<String, String> embeddedColumns = new HashMap<String, String>();
        for (SearchResult searchResult : searchResults)
        {
            if (searchResult.getEmbeddedColumnValues() != null)
            {
                for (String embeddedColVal : searchResult.getEmbeddedColumnValues())
                {
                    if (embeddedColVal != null)
                    {
                        StringBuilder strBuilder = new StringBuilder(embeddedColVal);
                        strBuilder.append("|");
                        strBuilder.append(searchResult.getPrimaryKey().toString());
                        embeddedColumns.put(strBuilder.toString(), searchResult.getPrimaryKey().toString());
                    }
                }
            }
        }
        List<EnhanceEntity> enhanceEntityList = new ArrayList<EnhanceEntity>();
        if (embeddedColumns != null && !embeddedColumns.isEmpty())
        {
            enhanceEntityList = client.find(m.getEntityClazz(), embeddedColumns);
        }
        else
        {
            for (SearchResult searchResult : searchResults)
            {
                primaryKeys.add(searchResult.getPrimaryKey());
            }
            enhanceEntityList = (List<EnhanceEntity>) ((CassandraClientBase) client).find(m.getEntityClazz(),
                    m.getRelationNames(), true, m, primaryKeys.toArray(new String[] {}));
        }

        return enhanceEntityList;
    }

    /**
     * Method to set indexcluase conditions.
     * 
     * @param conditions
     *            index conditions.
     */
    public void setConditions(Map<Boolean, List<IndexClause>> conditions)
    {
        this.conditions = conditions;
    }

    /**
     * Returns list of row keys. First element will be min value and second will
     * be major value.
     *
     * @param expressions the expressions
     * @param primaryKeyName the primary key name
     * @return the row key value
     */
    Map<String, byte[]> getRowKeyValue(List<IndexExpression> expressions, String primaryKeyName)
    {
        Map<String, byte[]> rowKeys = new HashMap<String, byte[]>();

        List<IndexExpression> rowExpressions = new ArrayList<IndexExpression>();

        if (expressions != null)
        {
            for (IndexExpression e : expressions)
            {
                if (primaryKeyName.equals(new String(e.getColumn_name())))
                {
                    IndexOperator operator = e.op;
                    if (operator.equals(IndexOperator.LTE) || operator.equals(IndexOperator.LT))
                    {
                        rowKeys.put(MAX_, e.getValue());
                        rowExpressions.add(e);
                    }
                    else if (operator.equals(IndexOperator.GTE) || operator.equals(IndexOperator.GT))
                    {
                        rowKeys.put(MIN_, e.getValue());
                        rowExpressions.add(e);
                    }
                    else if (operator.equals(IndexOperator.EQ))
                    {
                        rowKeys.put(MAX_, e.getValue());
                        rowKeys.put(MIN_, e.getValue());
                        rowExpressions.add(e);
                    }
                }
            }
            expressions.removeAll(rowExpressions);
        }
        return rowKeys;
    }
}