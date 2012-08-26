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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Query;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * The Class CassQuery.
 * 
 * @author vivek.mishra
 */
public class CassQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(CassQuery.class);

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new cass query.
     * 
     * @param query
     *            the query
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public CassQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        log.debug("on populateEntities cassandra query");
        List<Object> result = null;
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        if (appMetadata.isNative(getJPAQuery()))
        {
            result = ((PelopsClient) client).executeQuery(getJPAQuery(), m.getEntityClazz(), null);
        }
        else
        {
            if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
            {

                Map<Boolean, List<IndexClause>> ixClause = prepareIndexClause(m);
                boolean isRowKeyQuery = ixClause.keySet().iterator().next();
                if (!isRowKeyQuery)
                {
                    result = ((PelopsClient) client).find(ixClause.get(isRowKeyQuery), m, false, null, maxResult);
                }
                else
                {
                    result = ((CassandraEntityReader) getReader()).handleFindByRange(m, client, result, ixClause,
                            isRowKeyQuery);
                }

            }
            else
            {
                result = populateUsingLucene(m, client, result);

            }
        }
        return result;
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = null;
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        if (appMetadata.isNative(getJPAQuery()))
        {
            ls = (List<EnhanceEntity>) ((PelopsClient) client).executeQuery(getJPAQuery(), m.getEntityClazz(), null);
        }
        else
        {
            // Index in Inverted Index table if applicable
            boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m);

            if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty())
            {
                ls = ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, getKunderaQuery()
                        .getFilterClauseQueue());

            }
            else
            {
                Map<Boolean, List<IndexClause>> ixClause = MetadataUtils.useSecondryIndex(m.getPersistenceUnit()) ? prepareIndexClause(m)
                        : null;

                ((CassandraEntityReader) getReader()).setConditions(ixClause);

                ls = reader.populateRelation(m, client);
            }
        }
        return setRelationEntities(ls, client, m);

    }

    /**
     * On executeUpdate.
     * 
     * @return zero
     */
    @Override
    protected int onExecuteUpdate()
    {

        EntityMetadata m = getEntityMetadata();
        if (KunderaMetadata.INSTANCE.getApplicationMetadata().isNative(getJPAQuery()))
        {
            ((PelopsClient) persistenceDelegeator.getClient(m)).executeQuery(getJPAQuery(), m.getEntityClazz(), null);
        }
        else if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
            // throw new
            // QueryHandlerException("executeUpdate() is currently supported for native queries only");
        }

        return 0;
    }

    /**
     * Prepare index clause.
     * 
     * @param m
     *            the m
     * @return the map
     */
    private Map<Boolean, List<IndexClause>> prepareIndexClause(EntityMetadata m)
    {
        IndexClause indexClause = Selector.newIndexClause(Bytes.EMPTY, maxResult);
        List<IndexClause> clauses = new ArrayList<IndexClause>();
        List<IndexExpression> expr = new ArrayList<IndexExpression>();
        Map<Boolean, List<IndexClause>> idxClauses = new HashMap<Boolean, List<IndexClause>>(1);
        // check if id column are mixed with other columns or not?
        String idColumn = m.getIdColumn().getName();
        boolean idPresent = false;
        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                // String fieldName = getColumnName(clause.getProperty());
                String fieldName = clause.getProperty();
                // in case id column matches with field name, set it for first
                // time.
                if (!idPresent && idColumn.equalsIgnoreCase(fieldName))
                {
                    idPresent = true;
                }

                if (idPresent & !idColumn.equalsIgnoreCase(fieldName))
                {
                    log.error("Support for search on rowKey and indexed column is not enabled with in cassandra");
                    throw new QueryHandlerException("unsupported query operation clause for cassandra");

                }
                String condition = clause.getCondition();
                Object value = clause.getValue();
                // value.e
                /*
                 * if(idPresent) { expr = null; } else {
                 */
                expr.add(Selector.newIndexExpression(fieldName, getOperator(condition, idPresent),
                        getBytesValue(fieldName, m, value)));
                // }

            }
            else
            {
                // Case of AND and OR clause.
                String opr = o.toString();
                if (opr.equalsIgnoreCase("or"))
                {
                    log.error("Support for OR clause is not enabled with in cassandra");
                    throw new QueryHandlerException("unsupported clause " + opr + " for cassandra");
                }

            }
        }

        if (!StringUtils.isBlank(getKunderaQuery().getFilter()))
        {
            indexClause.setExpressions(expr);
            clauses.add(indexClause);
        }
        idxClauses.put(idPresent, clauses);

        return idxClauses;
    }

    /**
     * Gets the operator.
     * 
     * @param condition
     *            the condition
     * @param idPresent
     *            the id present
     * @return the operator
     */
    private IndexOperator getOperator(String condition, boolean idPresent)
    {
        if (/* !idPresent && */condition.equals("="))
        {
            return IndexOperator.EQ;
        }
        else if (!idPresent && condition.equals(">"))
        {
            return IndexOperator.GT;
        }
        else if (!idPresent && condition.equals("<"))
        {
            return IndexOperator.LT;
        }
        else if (condition.equals(">="))
        {
            return IndexOperator.GTE;
        }
        else if (condition.equals("<="))
        {
            return IndexOperator.LTE;
        }
        else
        {
            if (!idPresent)
            {
                throw new UnsupportedOperationException(" Condition " + condition + " is not suported in  cassandra!");
            }
            else
            {
                throw new UnsupportedOperationException(" Condition " + condition
                        + " is not suported for query on row key!");

            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        if (reader == null)
        {
            reader = new CassandraEntityReader(getLuceneQueryFromJPAQuery());
        }

        return reader;
    }

    /**
     * Returns bytes value for given value.
     * 
     * @param fieldName
     *            field name.
     * @param m
     *            entity metadata
     * @param value
     *            value.
     * @return bytes value.
     */
    private Bytes getBytesValue(String fieldName, EntityMetadata m, Object value)
    {
        Column idCol = m.getIdColumn();
        Field f = null;
        boolean isId = false;
        if (idCol.getName().equals(fieldName))
        {
            f = idCol.getField();
            isId = true;
        }
        else
        {
            Column col = m.getColumn(fieldName);
            if (col == null)
            {
                throw new QueryHandlerException("column type is null for: " + fieldName);
            }
            f = col.getField();
        }

        // need to do integer.parseInt..as value will be string in case of
        // create query.
        if (f != null && f.getType() != null)
        {
            if (/* isId || */f.getType().isAssignableFrom(String.class))
            {
                return Bytes.fromByteArray(((String) value).getBytes());
            }
            else if (f.getType().equals(int.class) || f.getType().isAssignableFrom(Integer.class))
            {
                return Bytes.fromInt(Integer.parseInt(value.toString()));
            }
            else if (f.getType().equals(long.class) || f.getType().isAssignableFrom(Long.class))
            {
                return Bytes.fromLong(Long.parseLong(value.toString()));
            }
            else if (f.getType().equals(boolean.class) || f.getType().isAssignableFrom(Boolean.class))
            {
                return Bytes.fromBoolean(Boolean.valueOf(value.toString()));
            }
            else if (f.getType().equals(double.class) || f.getType().isAssignableFrom(Double.class))
            {
                return Bytes.fromDouble(Double.valueOf(value.toString()));
            }
            else if (f.getType().isAssignableFrom(java.util.UUID.class))
            {
                return Bytes.fromUuid(UUID.fromString(value.toString()));
            }
            else if (f.getType().equals(float.class) || f.getType().isAssignableFrom(Float.class))
            {
                return Bytes.fromFloat(Float.valueOf(value.toString()));
            }
            else if (f.getType().isAssignableFrom(Date.class))
            {
                DateAccessor dateAccessor = new DateAccessor();
                return Bytes.fromByteArray(dateAccessor.toBytes(value));
            }
            else
            {
                if (value.getClass().isAssignableFrom(String.class))
                {
                    value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType().getClass(),
                            value.toString());
                }
                return Bytes.fromByteArray(PropertyAccessorFactory.getPropertyAccessor(f).toBytes(value));
            }
        }
        else
        {
            log.error("Error while handling data type for:" + fieldName);
            throw new QueryHandlerException("field type is null for:" + fieldName);
        }
    }
}
