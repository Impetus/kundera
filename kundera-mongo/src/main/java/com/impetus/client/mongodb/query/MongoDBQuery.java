/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.mongodb.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Pattern;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.SortOrder;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.query.exception.QueryHandlerException;
import com.mongodb.BasicDBObject;

/**
 * Query class for MongoDB data store
 * 
 * @author amresh.singh
 */
public class MongoDBQuery extends QueryImpl
{
    /** The log used by this class. */
    private static Log log = LogFactory.getLog(MongoDBQuery.class);

    public MongoDBQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String... persistenceUnits)
    {
        super(jpaQuery, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
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
        try
        {
            BasicDBObject orderByClause = getOrderByClause();
            return ((MongoDBClient) client).loadData(m, createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()),
                    getKunderaQuery().getResult(), null, orderByClause);
        }
        catch (Exception e)
        {
            throw new QueryHandlerException(e.getMessage());
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#handleAssociations(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client,
     * java.util.List, java.util.List, boolean)
     */
    @Override
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        // TODO : required to modify client return relation.
        // if it is a parent..then find data related to it only
        // else u need to load for associated fields too.
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();

        try
        {
            BasicDBObject orderByClause = getOrderByClause();
            ls = ((MongoDBClient) client).loadData(m, createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()),
                    getKunderaQuery().getResult(), relationNames, orderByClause);
        }
        catch (Exception e)
        {
            throw new QueryHandlerException(e.getMessage());
        }

        return handleGraph(ls, graphs, client, m);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new MongoEntityReader();
    }

    /**
     * Creates MongoDB Query object from filterClauseQueue
     * 
     * @param filterClauseQueue
     * @return
     */
    private BasicDBObject createMongoQuery(EntityMetadata m, Queue filterClauseQueue)
    {
        BasicDBObject query = new BasicDBObject();
        for (Object object : filterClauseQueue)
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = getColumnName(filter.getProperty());
                String condition = filter.getCondition();
                String value = filter.getValue();

                // Property, if doesn't exist in entity, may be there in a
                // document embedded within it, so we have to check that
                // TODO: Query should actually be in a format
                // documentName.embeddedDocumentName.column, remove below if
                // block once this is decided

                String enclosingDocumentName = getEnclosingDocumentName(m, property);
                if (enclosingDocumentName != null)
                {
                    property = enclosingDocumentName + "." + property;
                }

                if (condition.equals("="))
                {
                    query.append(property, value);
                }
                else if (condition.equalsIgnoreCase("like"))
                {
                    query.append(property, Pattern.compile(value));
                }
                else if (condition.equalsIgnoreCase(">"))
                {
                    if (query.containsField(property))
                    {
                        query.get(property);
                        query.put(property, ((BasicDBObject) query.get(property)).append("$gt", value));
                    }
                    else
                    {
                        query.append(property, new BasicDBObject("$gt", value));
                    }

                }
                else if (condition.equalsIgnoreCase(">="))
                {
                    if (query.containsField(property))
                    {
                        query.get(property);
                        query.put(property, ((BasicDBObject) query.get(property)).append("$gte", value));
                    }
                    else
                    {
                        query.append(property, new BasicDBObject("$gte", value));
                    }

                }
                else if (condition.equalsIgnoreCase("<"))
                {
                    if (query.containsField(property))
                    {
                        query.get(property);
                        query.put(property, ((BasicDBObject) query.get(property)).append("$lt", value));
                    }
                    else
                    {
                        query.append(property, new BasicDBObject("$lt", value));
                    }
                }
                else if (condition.equalsIgnoreCase("<="))
                {
                    if (query.containsField(property))
                    {
                        query.get(property);
                        query.put(property, ((BasicDBObject) query.get(property)).append("$lte", value));
                    }
                    else
                    {
                        query.append(property, new BasicDBObject("$lte", value));
                    }
                }

                // TODO: Add support for other operators like >, <, >=, <=,
                // order by asc/ desc, limit, skip, count etc
            }
        }
        return query;
    }

    /**
     * Prepare order by clause
     * 
     * @return order by clause.
     */
    private BasicDBObject getOrderByClause()
    {
        BasicDBObject orderByClause = null;

        List<SortOrdering> orders = kunderaQuery.getOrdering();
        if (orders != null)
        {
            orderByClause = new BasicDBObject();
            for (SortOrdering order : orders)
            {
                orderByClause.append(order.getColumnName(), order.getColumnName().equals(SortOrder.ASC) ? 1 : -1);
            }
        }

        return orderByClause;
    }

    /**
     * @param m
     * @param columnName
     * @param embeddedDocumentName
     * @return
     */
    private String getEnclosingDocumentName(EntityMetadata m, String columnName)
    {
        String enclosingDocumentName = null;
        if (!m.getColumnFieldNames().contains(columnName))
        {

            for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
            {
                List<Column> columns = superColumn.getColumns();
                for (Column column : columns)
                {
                    if (column.getName().equals(columnName))
                    {
                        enclosingDocumentName = superColumn.getName();
                        break;
                    }
                }
            }

        }
        return enclosingDocumentName;
    }
}
