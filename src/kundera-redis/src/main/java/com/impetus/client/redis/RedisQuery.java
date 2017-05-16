/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.s
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
package com.impetus.client.redis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.persistence.Query;

import com.impetus.client.redis.RedisQueryInterpreter.Clause;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * Redis query interface implements <code> {@link Query} </code> and extends
 * <code> {@link QueryImpl}</code>
 * 
 * @author vivek.mishra
 */
public class RedisQuery extends QueryImpl
{

    public RedisQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
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
    protected List<Object> populateEntities(EntityMetadata entityMetadata, Client client)
    {
        /**
         * Multiple clause: like two columns with and cluase For multiple
         * andTranslate jpa clause into temp store based on clause if it is for
         * multiple data set. and fetch from there and then finally delete that
         * temp storage. Query translator will translate queries into key value
         */

        /**
         * Limitations: 1) Multiple AND/OR clause not possible 2) Only equal
         * clause possible with AND/OR clause.
         * 
         */

        Set<Object> results = new LinkedHashSet<Object>();

        ClientMetadata clientMetadata = ((ClientBase) client).getClientMetadata();

        if (!MetadataUtils.useSecondryIndex(clientMetadata)
                && !(clientMetadata.getIndexImplementor() != null && clientMetadata.getIndexImplementor().equals(
                        RedisIndexer.class.getName())))
        {
            results.addAll(populateUsingLucene(entityMetadata, client, null,
                    getColumns(getKunderaQuery().getResult(), entityMetadata)));
        }
        else
        {
            RedisQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), entityMetadata);
            results.addAll((List<Object>) ((RedisClient) client).onExecuteQuery(interpreter,
                    entityMetadata.getEntityClazz()));
        }

        List<Object> output = new ArrayList<Object>();
        output.addAll(results);

        return output;
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
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
        RedisQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
        ls = ((RedisClient) client).onExecuteQuery(interpreter, m.getEntityClazz());
        return setRelationEntities(ls, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        // TODO Auto-generated method stub
        // WHY is it required!!!
        return new RedisEntityReader(kunderaQuery, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
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

    private RedisQueryInterpreter onTranslation(Queue clauseQueue, EntityMetadata entityMetadata)
    {
        RedisQueryInterpreter interpreter = new RedisQueryInterpreter(getColumns(getKunderaQuery().getResult(),
                entityMetadata));

        // If there is no clause present, means we might need to scan complete
        // table.
        /**
         * TODOOOO: Create a sorted set with table name. and add row key as
         * score and value on persist. delete it out as well on delete call.
         */
        for (Object clause : clauseQueue)
        {
            
            if (clause.toString().trim().equalsIgnoreCase("(") || clause.toString().trim().equalsIgnoreCase(")"))
            {
                // nested/multiple AND/OR not supported in Redis
                continue;
            }
            
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                Object value = ((FilterClause) clause).getValue().get(0);
                String condition = ((FilterClause) clause).getCondition();
                String columnName = ((FilterClause) clause).getProperty();

                if (columnName.equals(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()))
                {
                    interpreter.setById(true);
                }

                if (condition.equals("="))
                {
                    interpreter.setFieldName(columnName);
                    interpreter.setValue(value);
                }
                // TODO:: this is not possible. REDIS does not provide a way for
                // this even for numeric values, reason: it includes boundary
                // values by default.
                /*
                 * else if (condition.equals(">")) {
                 * interpreter.setMin(columnName,value); // these can only work
                 * for // numeric values.
                 * 
                 * } else if (condition.equals("<")) {
                 * interpreter.setMax(columnName,value); // these can only work
                 * for // numeric values.
                 * 
                 * }
                 */else if (condition.equals(">="))
                {
                    validateClause(interpreter, condition, columnName);
                    interpreter.setMin(columnName, value);
                    if (interpreter.getMax() == null)
                    {
                        interpreter.setMax(columnName, -1D);
                    }
                    // interp
                }
                else if (condition.equals("<="))
                {
                    validateClause(interpreter, condition, columnName);
                    interpreter.setMax(columnName, value);
                    if (interpreter.getMin() == null)
                    {
                        interpreter.setMin(columnName, 0D);
                    }
                }
                else if (interpreter.getClause() != null)
                {
                    throw new QueryHandlerException("Condition:" + condition
                            + " not supported for REDIS with nested AND/OR Clause.");
                }
                else
                {
                    throw new QueryHandlerException("Condition:" + condition + " not supported for REDIS");
                }
            }
            else
            {

                String opr = clause.toString().trim();

                if (interpreter.getClause() == null)
                {
                    if (opr.equalsIgnoreCase("AND"))
                    {
                        interpreter.setClause(Clause.INTERSECT);
                    }
                    else if (opr.equalsIgnoreCase("OR"))
                    {
                        interpreter.setClause(Clause.UNION);
                    }
                    else
                    {
                        throw new QueryHandlerException("Invalid intra clause:" + opr + " not supported for REDIS");
                    }

                }
                else if (RedisQueryInterpreter.getMappedClause(opr) == null)
                {
                    throw new QueryHandlerException("Invalid intra clause:" + opr + " not supported for REDIS");
                }
                else if (interpreter.getClause() != null
                        && !interpreter.getClause().equals(RedisQueryInterpreter.getMappedClause(opr)))
                {
                    throw new QueryHandlerException("Multiple combination of AND/OR clause not supported for REDIS");
                }
                // it is a case of "AND", "OR" clause
            }
        }

        return interpreter;
    }

    private void validateClause(RedisQueryInterpreter interpreter, String condition, String columnName)
    {
        if (interpreter.getClause() != null)
        {
            if (interpreter.getFieldName() != null && !interpreter.getFieldName().equals(columnName))
            {
                throw new QueryHandlerException(
                        "Nested AND/OR clause is not supported for different set of fields for condition:" + condition);
            }
        }

        interpreter.setFieldName(columnName);
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Iterator iterate()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {

        RedisQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
        return ((RedisClient) client).onExecuteQuery(interpreter, m.getEntityClazz());

    }

}