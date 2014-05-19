/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.persistence.Query;
import javax.persistence.metamodel.SingularAttribute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.OracleNoSQLClient;
import com.impetus.client.oraclenosql.OracleNoSQLEntityReader;
import com.impetus.client.oraclenosql.index.OracleNoSQLInvertedIndexer;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * Implementation of {@link Query} for Oracle NoSQL database
 * 
 * @author amresh.singh
 */
public class OracleNoSQLQuery extends QueryImpl
{
    private static Logger log = LoggerFactory.getLogger(OracleNoSQLQuery.class);

    public OracleNoSQLQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Populating entities for JPA query on OracleNOSQL");
        }

        Set<Object> results = new HashSet<Object>();

        OracleNoSQLQueryInterpreter interpreter = translateQuery(getKunderaQuery().getFilterClauseQueue(), m);

        Set<Object> resultsFromIdSearch = new HashSet<Object>();

        // Find By ID queries
        if (interpreter.isFindById())
        {
            Object entity = client.find(m.getEntityClazz(), interpreter.getRowKey());
            resultsFromIdSearch.add(entity);

            if (interpreter.getOperatorWithRowKey() == null)
            {
                List<Object> output = new ArrayList<Object>();
                output.addAll(resultsFromIdSearch);
                return output;
            }
        }

        ClientMetadata clientMetadata = ((ClientBase) client).getClientMetadata();

        if (!MetadataUtils.useSecondryIndex(clientMetadata)
                && !(clientMetadata.getIndexImplementor() != null && clientMetadata.getIndexImplementor().equals(
                        OracleNoSQLInvertedIndexer.class.getName())))
        {
            results.addAll(populateUsingLucene(m, client, null, interpreter.getSelectColumns()));
        }
        else
        {

            results.addAll((List<Object>) ((OracleNoSQLClient) client).executeQuery(m.getEntityClazz(), interpreter,
                    null));
        }

        List<Object> output = new ArrayList<Object>();
        output.addAll(results);

        return output;
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<Object> ls = new ArrayList<Object>();
        ls = populateEntities(m, client);

        return setRelationEntities(ls, client, m);
    }

    @Override
    protected EntityReader getReader()
    {
        return new OracleNoSQLEntityReader(kunderaQuery, kunderaMetadata);
    }

    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    private OracleNoSQLQueryInterpreter translateQuery(Queue clauseQueue, EntityMetadata entityMetadata)
    {
        OracleNoSQLQueryInterpreter interpreter = new OracleNoSQLQueryInterpreter(getColumns(getKunderaQuery()
                .getResult(), entityMetadata));
        interpreter.setClauseQueue(clauseQueue);

        String operatorWithIdClause = null;
        boolean idClauseFound = false;
        for (Object clause : clauseQueue)
        {
            if (clause.getClass().isAssignableFrom(FilterClause.class) && !idClauseFound)
            {
                String columnName = ((FilterClause) clause).getProperty();
                SingularAttribute idAttribute = entityMetadata.getIdAttribute();
                if (columnName.equals(((AbstractAttribute) idAttribute).getJPAColumnName()))
                {
                    interpreter.setFindById(true);
                    // To convert rowkey string to object.
                 // With 2.11 onwards Filter clause values has been changed to collection of values. other than IN or sub query
                    // doing get(0) here.
//                    Object keyObj = PropertyAccessorHelper.fromSourceToTargetClass(
//                            ((AbstractAttribute) idAttribute).getBindableJavaType(), String.class,
//                            ((FilterClause) clause).getValue().get(0));
                    
                    interpreter.setRowKey((((FilterClause) clause).getValue().get(0))/*
                                                 * ((FilterClause)
                                                 * clause).getValue()
                                                 */);
                    idClauseFound = true;
                }
            }
            else if (clause instanceof String)
            {
                operatorWithIdClause = clause.toString().trim();
            }

            if (idClauseFound && operatorWithIdClause != null)
            {
                break;
            }
        }

        interpreter.setOperatorWithRowKey(operatorWithIdClause);

        return interpreter;
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
}
