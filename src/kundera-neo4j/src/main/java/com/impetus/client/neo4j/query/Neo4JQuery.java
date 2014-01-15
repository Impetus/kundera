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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.impetus.client.neo4j.Neo4JClient;
import com.impetus.client.neo4j.Neo4JEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * Neo4J Query Implementor
 * 
 * @author amresh.singh
 */
public class Neo4JQuery extends QueryImpl
{
    private static final String NATIVE_QUERY_TYPE = "native.query.type";

    Neo4JQueryType queryType;

    /** The reader. */
    private EntityReader reader;

    /**
     * @param query
     * @param persistenceDelegator
     */
    public Neo4JQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(kunderaQuery, persistenceDelegator);
        if (getHints().containsKey(NATIVE_QUERY_TYPE))
        {
            queryType = (Neo4JQueryType) getHints().get(NATIVE_QUERY_TYPE);
        }
        else
        {
            queryType = Neo4JQueryType.LUCENE;
        }

    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        // One implementation for entities with or without relations
        return recursivelyPopulateEntities(m, client);
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<Object> entities = new ArrayList<Object>();
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        String query = appMetadata.getQuery(getJPAQuery());
        boolean isNative = kunderaQuery.isNative()/*query == null ? true : appMetadata.isNative(getJPAQuery())*/;        

        if (isNative)
        {
            String nativeQuery = query != null ? query : getJPAQuery();
            Neo4JNativeQuery nativeQueryImpl = Neo4JNativeQueryFactory.getNativeQueryImplementation(queryType);
            entities = nativeQueryImpl.executeNativeQuery(nativeQuery, (Neo4JClient) client, m);
        }
        else
        {
            String luceneQuery = getLuceneQuery(kunderaQuery);
            entities = ((Neo4JClient) client).executeLuceneQuery(m, luceneQuery);
        }
        return setRelationEntities(entities, client, m);
    }

    @Override
    protected EntityReader getReader()
    {
        if (reader == null)
        {
            reader = new Neo4JEntityReader(kunderaQuery);
        }
        return reader;
    }

    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    private String getLuceneQuery(KunderaQuery kunderaQuery)
    {
        StringBuffer sb = new StringBuffer();

        if (kunderaQuery.getFilterClauseQueue().isEmpty())
        {
            // Select All query if filter clause is empty
            String idColumnName = ((AbstractAttribute) kunderaQuery.getEntityMetadata().getIdAttribute())
                    .getJPAColumnName();
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
                    // property
                    sb.append(filter.getProperty());

                    // joiner
                    String appender = "";
                    String condition = filter.getCondition().trim();
                    if (condition.equals("="))
                    {
                        sb.append(":");
                    }
                    else if (condition.equalsIgnoreCase("like"))
                    {
                        sb.append(":");
                        appender = "*";
                    }
                    else if (condition.equalsIgnoreCase(">"))
                    {
                        // TODO: Amresh need to look for "String.class"
                        // parameter.
                        sb.append(appendRange(filter.getValue().toString(), false, true, String.class));
                        appended = true;
                    }
                    else if (condition.equalsIgnoreCase(">="))
                    {
                        sb.append(appendRange(filter.getValue().toString(), true, true, String.class));
                        appended = true;
                    }
                    else if (condition.equalsIgnoreCase("<"))
                    {
                        sb.append(appendRange(filter.getValue().toString(), false, false, String.class));
                        appended = true;
                    }
                    else if (condition.equalsIgnoreCase("<="))
                    {
                        sb.append(appendRange(filter.getValue().toString(), true, false, String.class));
                        appended = true;
                    }

                    // value. if not already appended.
                    if (!appended)
                    {
                        if (appender.equals("") && filter.getValue() != null
                                && filter.getValue().toString().contains(" "))
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


    /**
     * Append range.
     * 
     * @param value
     *            the value
     * @param inclusive
     *            the inclusive
     * @param isGreaterThan
     *            the is greater than
     * @return the string
     */
    protected String appendRange(String value, boolean inclusive, boolean isGreaterThan, Class clazz)
    {
        String appender = " ";
        StringBuilder sb = new StringBuilder();
        sb.append(":");
        sb.append(inclusive ? "[" : "{");
        sb.append(isGreaterThan ? value : "*");
        sb.append(appender);
        sb.append("TO");
        sb.append(appender);
        if (clazz.isAssignableFrom(int.class) || clazz.isAssignableFrom(Integer.class)
                || clazz.isAssignableFrom(short.class) || clazz.isAssignableFrom(long.class)
                || clazz.isAssignableFrom(Long.class) || clazz.isAssignableFrom(float.class)
                || clazz.isAssignableFrom(Float.class) || clazz.isAssignableFrom(BigDecimal.class)
                || clazz.isAssignableFrom(BigInteger.class)
                || clazz.isAssignableFrom(Double.class)
                || clazz.isAssignableFrom(double.class))
        {
            sb.append(isGreaterThan ? "*" : value);

        }
        else
        {
            sb.append(isGreaterThan ? "null" : value);
        }

        // sb.append(isGreaterThan ? "null" : value);

        sb.append(inclusive ? "]" : "}");
        return sb.toString();
    }

}
