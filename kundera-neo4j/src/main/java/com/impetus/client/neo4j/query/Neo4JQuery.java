/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.neo4j.query;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Query;

import com.impetus.client.neo4j.Neo4JClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * Neo4J Query Implementor 
 * @author amresh.singh
 */
public class Neo4JQuery extends QueryImpl implements Query
{
    /**
     * @param query
     * @param persistenceDelegator
     */
    public Neo4JQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
        
    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        return null;
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<Object> entities = new ArrayList<Object>();
        
        String luceneQuery = getLuceneQuery(kunderaQuery);
        entities = ((Neo4JClient) client).executeLuceneQuery(m, luceneQuery);             
        
        return entities;
    }

    @Override
    protected EntityReader getReader()
    {
        return null;
    }

    @Override
    protected int onExecuteUpdate()
    {
        return 0;
    }
    
    private String getLuceneQuery(KunderaQuery kunderaQuery)
    {
        StringBuffer sb = new StringBuffer();
        
        if(kunderaQuery.getFilterClauseQueue().isEmpty())
        {           
            //Select All query if filter clause is empty
            String idColumnName = ((AbstractAttribute)kunderaQuery.getEntityMetadata().getIdAttribute()).getJPAColumnName();
            sb.append(idColumnName).append(":").append("*");
        }
        else
        {
            for (Object object : kunderaQuery.getFilterClauseQueue())
            {
                if (object instanceof FilterClause)
                {
                    boolean appended = false;
                    FilterClause filter = (FilterClause) object;
                    //sb.append("+");
                    // property
                    sb.append(filter.getProperty());

                    // joiner
                    String appender = "";
                    if (filter.getCondition().equals("="))
                    {
                        sb.append(":");
                    }
                    else if (filter.getCondition().equalsIgnoreCase("like"))
                    {
                        sb.append(":");
                        appender = "*";
                    }
                    else if (filter.getCondition().equalsIgnoreCase(">"))
                    {
                        sb.append(appendRange(filter.getValue().toString(), false, true));
                        appended = true;
                    }
                    else if (filter.getCondition().equalsIgnoreCase(">="))
                    {
                        sb.append(appendRange(filter.getValue().toString(), true, true));
                        appended = true;
                    }
                    else if (filter.getCondition().equalsIgnoreCase("<"))
                    {
                        sb.append(appendRange(filter.getValue().toString(), false, false));
                        appended = true;
                    }
                    else if (filter.getCondition().equalsIgnoreCase("<="))
                    {
                        sb.append(appendRange(filter.getValue().toString(), true, false));
                        appended = true;
                    }

                    // value. if not already appended.
                    if (!appended)
                    {
                        if(appender.equals("") && filter.getValue() != null && filter.getValue().toString().contains(" "))
                        {
                            sb.append("\"");
                            sb.append(filter.getValue().toString());
                            sb.append("\"");
                        }
                        else
                        {
                            sb.append(filter.getValue());
                            sb.append(appender);
                        }       
                        
                    }
                }
                else
                {
                    sb.append(" " + object + " ");
                }
            } 
        }       
       
        return sb.toString();
    }
    
}
