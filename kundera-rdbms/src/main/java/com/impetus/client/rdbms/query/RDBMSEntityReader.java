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
package com.impetus.client.rdbms.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * The Class RDBMSEntityReader.
 * 
 * @author vivek.mishra
 */
public class RDBMSEntityReader extends AbstractEntityReader implements EntityReader
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(RDBMSEntityReader.class);

    /** The conditions. */
    private Queue conditions;

    /** The filter. */
    private String filter;

    /** The jpa query. */
    private String jpaQuery;

    /**
     * Instantiates a new rDBMS entity reader.
     * 
     * @param luceneQuery
     *            the lucene query
     * @param query
     *            the query
     */
    public RDBMSEntityReader(String luceneQuery, String query)
    {
        this.luceneQueryFromJPAQuery = luceneQuery;
        this.jpaQuery = query;
    }

    /**
     * Instantiates a new rDBMS entity reader.
     */
    public RDBMSEntityReader()
    {
        
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus
     * .kundera.metadata.model.EntityMetadata, java.util.List, boolean,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, List<String> relationNames, boolean isParent,
            Client client)
    {
        List<EnhanceEntity> ls = null;
        if (!isParent)
        {
            // if it is not a parent.
            String sqlQuery = null;
            if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
            {
                sqlQuery = getSqlQueryFromJPA(m, relationNames, null);
            }
            else
            {
                // prepare lucene query and find.
                Set<String> rSet = fetchDataFromLucene(client);
                sqlQuery = getSqlQueryFromJPA(m, relationNames, rSet);

            }
            // call client with relation name list and convert to sql query.

            ls = populateEnhanceEntities(m, relationNames, client, sqlQuery);

        }
        else
        {
            if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
            {
                try
                {
                    List entities = ((HibernateClient) client).find(jpaQuery, new ArrayList<String>(),
                            m.getEntityClazz());
                    ls = new ArrayList<EnhanceEntity>(entities.size());
                    transform(m, ls, entities);
                }
                catch (Exception e)
                {
                    log.error("Error while executing handleAssociation for cassandra:" + e.getMessage());
                    throw new QueryHandlerException(e.getMessage());
                }
            }
            else
            {
                onAssociationUsingLucene(m, client, ls);
            }

        }

        return ls;
    }

    private List<EnhanceEntity> populateEnhanceEntities(EntityMetadata m, List<String> relationNames, 
                                                        Client client, String sqlQuery)
    {
        List<EnhanceEntity> ls;
        List<Object[]> result = ((HibernateClient) client).find(sqlQuery, relationNames, m.getEntityClazz());

        ls = new ArrayList<EnhanceEntity>(result.size());
        for (Object[] o : result)
        {
            EnhanceEntity e = new EnhanceEntity(o[0], getId(o[0], m), populateRelations(relationNames, o));
            ls.add(e);
        }
        return ls;
    }

    /**
     * Gets the sql query from jpa.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param relations
     *            the relations
     * @param primaryKeys
     *            the primary keys
     * @return the sql query from jpa
     */
    public String getSqlQueryFromJPA(EntityMetadata entityMetadata, List<String> relations, Set<String> primaryKeys)
    {
        String aliasName = "_" + entityMetadata.getTableName();

        StringBuilder queryBuilder = new StringBuilder("Select ");

        queryBuilder.append(aliasName);
        queryBuilder.append(".");
        queryBuilder.append(entityMetadata.getIdColumn().getName());

        for (String column : entityMetadata.getColumnFieldNames())
        {
            queryBuilder.append(", ");
            queryBuilder.append(aliasName);
            queryBuilder.append(".");
            queryBuilder.append(column);
        }
        for (String relation : relations)
        {
            queryBuilder.append(", ");
            queryBuilder.append(relation);
        }
        queryBuilder.append(" From ");
        queryBuilder.append(entityMetadata.getTableName());
        queryBuilder.append(" ");
        queryBuilder.append(aliasName);

        // add conditions
        if (filter != null)
        {
            queryBuilder.append(" Where ");
        }

        if (primaryKeys == null)
        {
            for (Object o : conditions)
            {

                if (o instanceof FilterClause)
                {
                    FilterClause clause = ((FilterClause) o);
                    queryBuilder.append(StringUtils.replace(clause.getProperty(),
                            clause.getProperty().substring(0, clause.getProperty().indexOf(".")), aliasName));
                    queryBuilder.append(" ");
                    queryBuilder.append(clause.getCondition());

                    if (clause.getCondition().equalsIgnoreCase("like"))
                    {
                        queryBuilder.append("%");
                    }
                    queryBuilder.append(" ");
                    queryBuilder.append(clause.getValue());

                }
                else
                {
                    queryBuilder.append(" ");
                    queryBuilder.append(o);
                    queryBuilder.append(" ");
                }

            }
        }
        else
        {
            
            queryBuilder.append(aliasName);
            queryBuilder.append(".");
            queryBuilder.append(entityMetadata.getIdColumn().getName());
            queryBuilder.append(" ");
            queryBuilder.append("IN(");
            int count = 0;
            for (String key : primaryKeys)
            {
                queryBuilder.append(key);
                if (++count != primaryKeys.size())
                {
                    queryBuilder.append(",");
                } else {
                    queryBuilder.append(")");
                }
            }
            

        }
        return queryBuilder.toString();
    }

    /**
     * Sets the conditions.
     * 
     * @param q
     *            the new conditions
     */
    public void setConditions(Queue q)
    {
        this.conditions = q;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    /**
     * Populate relations.
     * 
     * @param relations
     *            the relations
     * @param o
     *            the o
     * @return the map
     */
    private Map<String, Object> populateRelations(List<String> relations, Object[] o)
    {
        Map<String, Object> relationVal = new HashMap<String, Object>(relations.size());
        int counter = 1;
        for (String r : relations)
        {
            relationVal.put(r, o[counter++]);
        }
        return relationVal;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.EntityReader#findById(java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata, java.util.List, com.impetus.kundera.client.Client)
     */
    @Override
    public EnhanceEntity findById(String primaryKey, EntityMetadata m, List<String> relationNames, Client client)
    {
        if(relationNames != null && !relationNames.isEmpty())
        {
            Set<String> keys = new HashSet<String>(1);
            keys.add(primaryKey);
            String query = getSqlQueryFromJPA(m, relationNames, keys);
            return populateEnhanceEntities(m, relationNames, client, query).get(0);
        } else
        {
            Object o;
            try
            {
                o = client.find(m.getEntityClazz(), primaryKey, null);
            }
            catch (Exception e)
            {
                throw new PersistenceException(e.getMessage());
            }
            return o != null? new EnhanceEntity(o, getId(o, m), null):null;
        }
    }

}
