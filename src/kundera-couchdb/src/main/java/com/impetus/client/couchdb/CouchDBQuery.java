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
package com.impetus.client.couchdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.eclipse.persistence.jpa.jpql.parser.AvgFunction;
import org.eclipse.persistence.jpa.jpql.parser.CountFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.MaxFunction;
import org.eclipse.persistence.jpa.jpql.parser.MinFunction;
import org.eclipse.persistence.jpa.jpql.parser.NullExpression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.SumFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * Extends QueryImpl for CouchDB query.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBQuery extends QueryImpl
{

    /** The use lucene or es. */
    private boolean useLuceneOrES;

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(CouchDBQuery.class);

    /**
     * Instantiates a new couch db query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchDBQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /**
     * Populate results.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        ClientMetadata clientMetadata = ((ClientBase) client).getClientMetadata();
        this.useLuceneOrES = !MetadataUtils.useSecondryIndex(clientMetadata);
        if (useLuceneOrES)
        {
            return populateUsingLucene(m, client, null, kunderaQuery.getResult());
        }
        else
        {
            CouchDBQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
            return ((CouchDBClient) client).createAndExecuteQuery(interpreter);
        }
    }

    /**
     * Recursively populate entity.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = populateEntities(m, client);
        return setRelationEntities(ls, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#executeUpdate()
     */
    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new CouchDBEntityReader(kunderaQuery, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate()
    {
        return new ResultIterator((CouchDBClient) persistenceDelegeator.getClient(getEntityMetadata()),
                getEntityMetadata(), persistenceDelegeator, onTranslation(getKunderaQuery().getFilterClauseQueue(),
                        getEntityMetadata()), getFetchSize() != null ? getFetchSize() : this.maxResult);
    }

    /**
     * On translation.
     * 
     * @param clauseQueue
     *            the clause queue
     * @param m
     *            the m
     * @return the couch db query interpreter
     */
    private CouchDBQueryInterpreter onTranslation(Queue clauseQueue, EntityMetadata m)
    {

        CouchDBQueryInterpreter interpreter = new CouchDBQueryInterpreter(getColumns(getKunderaQuery().getResult(), m),
                getMaxResults(), m);
        interpreter.setColumnsToOutput(getColumnsToOutput(m, kunderaQuery));
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        if (getKunderaQuery().isAggregated() && !useLuceneOrES)
        {
            return onAggregatedQuery(m, interpreter, getKunderaQuery());
        }
        // If there is no clause present, means we might need to scan complete
        // table.
        /**
         * TODOOOO: Create a sorted set with table name. and add row key as
         * score and value on persist. delete it out as well on delete call.
         */
        for (Object clause : clauseQueue)
        {
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                Object value = ((FilterClause) clause).getValue().get(0);
                String condition = ((FilterClause) clause).getCondition();
                String columnName = ((FilterClause) clause).getProperty();

                int indexOfDot = columnName.indexOf(".");
                if (indexOfDot >= 0)
                {
                    EmbeddableType embeddableType = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
                    String embeddedeFieldName = columnName.substring(0, indexOfDot);
                    if (embeddedeFieldName.equals(((AbstractAttribute) m.getIdAttribute()).getName()))
                    {
                        interpreter.setQueryOnCompositeKey(true);
                        interpreter.setKeyName(embeddedeFieldName);
                        String colName = columnName.substring(indexOfDot + 1);
                        Attribute attribute = embeddableType.getAttribute(colName);
                        interpreter.setIdQuery(true);
                        interpreter.setKeyValues(
                                colName,
                                PropertyAccessorHelper.fromSourceToTargetClass(attribute.getJavaType(),
                                        value.getClass(), value));
                    }
                    else
                    {
                        log.error("Query on embedded column/any field of embedded column, is not supported in CouchDB");
                        throw new QueryHandlerException(
                                "Query on embedded column/any field of embedded column, is not supported in CouchDB");
                    }
                }
                else
                {
                    Attribute col = entity.getAttribute(m.getFieldName(columnName));
                    interpreter.setKeyValues(columnName,
                            PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(), value.getClass(), value));
                    interpreter.setKeyName(columnName);
                    if (columnName.equals(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()))
                    {
                        interpreter.setIdQuery(true);
                    }

                    if (condition.equals("="))
                    {
                        interpreter.setKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                                value.getClass(), value));
                    }
                    else if (condition.equals(">=") || condition.equals(">"))
                    {
                        interpreter.setStartKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                                value.getClass(), value));
                    }
                    else if (condition.equals("<="))
                    {
                        interpreter.setEndKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                                value.getClass(), value));
                    }
                    else if (condition.equals("<"))
                    {
                        interpreter.setEndKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                                value.getClass(), value));
                        interpreter.setIncludeLastKey(false);
                    }
                    else
                    {
                        log.error("Condition:" + condition + " not supported for CouchDB");
                        throw new QueryHandlerException("Condition:" + condition + " not supported for CouchDB");
                    }
                }
            }
            else
            {
                String opr = clause.toString().trim();

                if (interpreter.getOperator() == null)
                {
                    if (opr.equalsIgnoreCase("AND"))
                    {
                        interpreter.setOperator("AND");
                    }
                    else if (opr.equalsIgnoreCase("OR"))
                    {
                        log.error("Condition: OR not supported in CouchDB");
                        throw new QueryHandlerException("Invalid intra clause OR is not supported in CouchDB");
                    }
                    else
                    {
                        log.error("Invalid intra clause:" + opr + " is not supported in CouchDB");
                        throw new QueryHandlerException("Invalid intra clause:" + opr + " not supported for CouchDB");
                    }
                }
                else if (interpreter.getOperator() != null && !interpreter.getOperator().equalsIgnoreCase(opr))
                {
                    log.error("Multiple combination of AND/OR clause not supported in CouchDB");
                    throw new QueryHandlerException("Multiple combination of AND/OR clause not supported in CouchDB");
                }
                // it is a case of "AND", "OR" clause
            }
        }
        return interpreter;
    }

    /**
     * On aggregated query.
     * 
     * @param m
     *            the m
     * @param interpreter
     *            the interpreter
     * @param kunderaQuery
     *            the kundera query
     * @return the couch db query interpreter
     */
    private CouchDBQueryInterpreter onAggregatedQuery(EntityMetadata m, CouchDBQueryInterpreter interpreter,
            KunderaQuery kunderaQuery)
    {
        interpreter.setAggregation(true);
        SelectStatement selectStatement = kunderaQuery.getSelectStatement();
        Expression whereClause = selectStatement.getWhereClause();
        if (!NullExpression.class.isAssignableFrom(whereClause.getClass()))
        {
            throw new KunderaException("Aggregations with where clause are yet not supported in CouchDB");
        }
        SelectClause selectClause = (SelectClause) selectStatement.getSelectClause();
        Expression expression = selectClause.getSelectExpression();
        if (CountFunction.class.isAssignableFrom(expression.getClass()))
        {
            interpreter.setAggregationType(CouchDBConstants.COUNT);
            Expression exp = ((CountFunction) expression).getExpression();
            setAggregationColInInterpreter(m, interpreter, exp);
        }
        else if (MinFunction.class.isAssignableFrom(expression.getClass()))
        {
            interpreter.setAggregationType(CouchDBConstants.MIN);
            Expression exp = ((MinFunction) expression).getExpression();
            setAggregationColInInterpreter(m, interpreter, exp);
        }
        else if (MaxFunction.class.isAssignableFrom(expression.getClass()))
        {
            interpreter.setAggregationType(CouchDBConstants.MAX);
            Expression exp = ((MaxFunction) expression).getExpression();
            setAggregationColInInterpreter(m, interpreter, exp);
        }
        else if (AvgFunction.class.isAssignableFrom(expression.getClass()))
        {
            interpreter.setAggregationType(CouchDBConstants.AVG);
            Expression exp = ((AvgFunction) expression).getExpression();
            setAggregationColInInterpreter(m, interpreter, exp);
        }
        else if (SumFunction.class.isAssignableFrom(expression.getClass()))
        {
            interpreter.setAggregationType(CouchDBConstants.SUM);
            Expression exp = ((SumFunction) expression).getExpression();
            setAggregationColInInterpreter(m, interpreter, exp);
        }
        else
        {
            throw new KunderaException("This query is currently not supported in CouchDB");
        }
        return interpreter;
    }

    /**
     * Sets the aggregation col in interpreter.
     * 
     * @param m
     *            the m
     * @param interpreter
     *            the interpreter
     * @param exp
     *            the exp
     */
    private void setAggregationColInInterpreter(EntityMetadata m, CouchDBQueryInterpreter interpreter, Expression exp)
    {
        if (StateFieldPathExpression.class.isAssignableFrom(exp.getClass()))
        {
            Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(exp, m, kunderaMetadata);
            interpreter.setAggregationColumn((String) map.get(Constants.COL_NAME));
        }
    }

    /**
     * Gets the columns to output.
     * 
     * @param m
     *            the m
     * @param kunderaQuery
     *            the kundera query
     * @return the columns to output
     */
    private List<Map<String, Object>> getColumnsToOutput(EntityMetadata m, KunderaQuery kunderaQuery)
    {
        if (kunderaQuery.isSelectStatement())
        {
            SelectStatement selectStatement = kunderaQuery.getSelectStatement();
            SelectClause selectClause = (SelectClause) selectStatement.getSelectClause();
            return KunderaQueryUtils.readSelectClause(selectClause.getSelectExpression(), m, false, kunderaMetadata);
        }
        return new ArrayList();
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
        CouchDBQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
        return ((CouchDBClient) client).createAndExecuteQuery(interpreter);
    }

}