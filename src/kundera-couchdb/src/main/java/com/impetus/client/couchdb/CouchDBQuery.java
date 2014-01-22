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
import java.util.Queue;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
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
    private static final Logger log = LoggerFactory.getLogger(CouchDBQuery.class);

    public CouchDBQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(kunderaQuery, persistenceDelegator);
    }

    /**
     * Populate results.
     */
    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        CouchDBQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
        return ((CouchDBClient) client).createAndExecuteQuery(interpreter);
    }

    /**
     * Recursively populate entity.
     */
    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
        ls = populateEntities(m, client);
        return setRelationEntities(ls, client, m);
    }

    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    @Override
    protected EntityReader getReader()
    {
        return new CouchDBEntityReader(kunderaQuery);
    }

    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

    @Override
    public Iterator iterate()
    {
        return new ResultIterator((CouchDBClient) persistenceDelegeator.getClient(getEntityMetadata()),
                getEntityMetadata(), persistenceDelegeator, onTranslation(getKunderaQuery().getFilterClauseQueue(),
                        getEntityMetadata()), getFetchSize() != null ? getFetchSize() : this.maxResult);
    }

    /**
     * 
     * @param clauseQueue
     * @param m
     * @return
     */
    private CouchDBQueryInterpreter onTranslation(Queue clauseQueue, EntityMetadata m)
    {

        CouchDBQueryInterpreter interpreter = new CouchDBQueryInterpreter(getColumns(getKunderaQuery().getResult(), m),
                getMaxResults(), m);
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());

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
                Object value = ((FilterClause) clause).getValue();
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
}