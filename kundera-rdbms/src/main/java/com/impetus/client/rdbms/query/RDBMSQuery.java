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
import java.util.List;
import java.util.Queue;

import javax.persistence.Query;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * The Class RDBMSQuery.
 * 
 * @author vivek.mishra
 */
public class RDBMSQuery extends QueryImpl implements Query
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(RDBMSQuery.class);

    /**
     * Instantiates a new rDBMS query.
     * 
     * @param query
     *            the query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public RDBMSQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, String[] persistenceUnits)
    {
        super(query, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }


    /**
     * Handle associations.
     *
     * @param m the m
     * @param client the client
     * @param graphs the graphs
     * @param relationNames the relation names
     * @param isParent the is parent
     */
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        log.debug("on handleAssociations rdbms query");
        List<EnhanceEntity> ls = null;

        if (!isParent)
        {
            // if it is not a parent.
            String sqlQuery = getSqlQueryFromJPA(m, relationNames);

            // call client with relation name list and convert to sql query.

            List<Object[]> result = ((HibernateClient) client).find(sqlQuery, relationNames, m.getEntityClazz());

            ls = new ArrayList<EnhanceEntity>(result.size());
            for (Object[] o : result)
            {
                EnhanceEntity e = new EnhanceEntity(o[0], persistenceDelegeator.getId(o[0], m), populateRelations(
                        relationNames, o));
                ls.add(e);
            }
        }
        else
        {
            try
            {
                List entities = client.loadData(this);
                ls = new ArrayList<EnhanceEntity>(entities.size());
                for (Object o : entities)
                {
                    EnhanceEntity e = new EnhanceEntity(o, persistenceDelegeator.getId(o, m), null);
                    ls.add(e);
                }
            }
            catch (Exception e)
            {
                // TODO This is really bad way to throw away exceptions from
                // API!
                e.printStackTrace();
            }

        }
        // pass graph and list of enhanced entities and graph for association
        // population.
        return handleGraph(ls, graphs);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        log.debug("on populateEntities rdbms query");
        try
        {
            List entities = client.loadData(this);
            persistenceDelegeator.store(entities, m);
            return entities;
        }
        catch (Exception e)
        {
            // TODO This is really bad way to throw away exceptions from API!
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the sql query from jpa.
     *
     * @param entityMetadata the entity metadata
     * @param relations the relations
     * @return the sql query from jpa
     */
    private String getSqlQueryFromJPA(EntityMetadata entityMetadata, List<String> relations)
    {
        String filter = getKunderaQuery().getFilter();
        Queue q = getKunderaQuery().getFilterClauseQueue();
        String aliasName = "_" + entityMetadata.getTableName();
        
        ;
        
        StringBuilder queryBuilder = new StringBuilder("Select ");
        
        queryBuilder.append(aliasName);
        queryBuilder.append(".");
        queryBuilder.append(entityMetadata.getIdColumn().getName());
        
        for(String column : entityMetadata.getColumnFieldNames())
        {
            queryBuilder.append(", ");
            queryBuilder.append(aliasName);
            queryBuilder.append(".");
            queryBuilder.append(column);
        }
//        queryBuilder.append(".*");
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
        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {

            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
//                queryBuilder.append(clause.getProperty());
               queryBuilder.append(StringUtils.replace(clause.getProperty(), clause.getProperty().substring(0,clause.getProperty().indexOf(".")), aliasName));
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

        return queryBuilder.toString();
    }

}
