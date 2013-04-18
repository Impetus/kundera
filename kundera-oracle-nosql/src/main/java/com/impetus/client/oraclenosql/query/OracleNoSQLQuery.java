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
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.oraclenosql.OracleNoSQLClient;
import com.impetus.client.oraclenosql.OracleNoSQLEntityReader;
import com.impetus.client.oraclenosql.index.OracleNoSQLInvertedIndexer;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * Implementation of {@link Query} for Oracle NoSQL database 
 * @author amresh.singh
 */
public class OracleNoSQLQuery extends QueryImpl implements Query
{
    private static Log log = LogFactory.getLog(OracleNoSQLQuery.class);
    
    public OracleNoSQLQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
        
    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Populating entities for JPA query on OracleNOSQL");
        }
        
        List<Object> results = new ArrayList<Object>();
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        String indexerClass = KunderaMetadata.INSTANCE.getApplicationMetadata()
        .getPersistenceUnitMetadata(m.getPersistenceUnit()).getProperties().getProperty(PersistenceProperties.KUNDERA_INDEXER_CLASS);
       
        ClientMetadata clientMetadata = KunderaMetadata.INSTANCE.getClientMetadata(m.getPersistenceUnit());
        
        if (clientMetadata.getIndexImplementor() != null && client.getIndexManager().getIndexer().getClass().equals(LuceneIndexer.class))
        {
            results = populateUsingLucene(m, client, results);             

        }
        else
        {
            Queue filterClauseQueue = getKunderaQuery().getFilterClauseQueue();
            
            Set<Object> primaryKeys = null;
            
            if(! filterClauseQueue.isEmpty())   //Select all query
            {
                //Select Query with where clause (requires search within inverted index)
                
                OracleNoSQLQueryInterpreter interpreter = translateQuery(getKunderaQuery().getFilterClauseQueue(), m);                
                
                primaryKeys = ((OracleNoSQLInvertedIndexer) client.getIndexManager().getIndexer()).executeQuery(interpreter, m.getEntityClazz());
                
            }    
                
            results = (List<Object>) ((OracleNoSQLClient) client).findAll(m.getEntityClazz(), primaryKeys == null ? null : primaryKeys.toArray());          
        }        
        return results;
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        return null;
    }

    @Override
    protected EntityReader getReader()
    {
        return new OracleNoSQLEntityReader(getLuceneQueryFromJPAQuery());
    }

    @Override
    protected int onExecuteUpdate()
    {
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
        }

        return 0;
    }  
    
    
    private OracleNoSQLQueryInterpreter translateQuery(Queue clauseQueue, EntityMetadata entityMetadata)
    {
        OracleNoSQLQueryInterpreter interpreter = new OracleNoSQLQueryInterpreter(getColumns(getKunderaQuery().getResult(), entityMetadata));

        // TODO: If there is no clause present, means we might need to scan complete table.
        for (Object clause : clauseQueue)
        {
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                Object value = ((FilterClause) clause).getValue();
                String condition = ((FilterClause) clause).getCondition();
                String columnName = ((FilterClause) clause).getProperty();

                if (columnName.equals(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()))
                {
                    interpreter.setById(true);
                }

                if (condition.equals("="))
                {
                    interpreter.addFilterCondition(columnName, value);
                }             
            }
            else
            {

                String operator = clause.toString();
                if (interpreter.getOperator() == null)
                {
                    interpreter.setOperator(operator);
                }                
            }
        }

        return interpreter;
    }   

}
