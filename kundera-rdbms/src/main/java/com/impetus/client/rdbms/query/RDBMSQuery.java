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

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * The Class RDBMSQuery.
 * 
 * @author vivek.mishra
 */
public class RDBMSQuery extends QueryImpl implements Query
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(RDBMSQuery.class);

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new rDBMS query.
     * 
     * @param query
     *            the query
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public RDBMSQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String[] persistenceUnits)
    {
        super(query, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    /**
     * Handle associations.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param graphs
     *            the graphs
     * @param relationNames
     *            the relation names
     * @param isParent
     *            the is parent
     * @return the list
     */
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        // retrieve
        log.debug("On handleAssociation() retrieve associations ");

        ((RDBMSEntityReader) getReader()).setConditions(getKunderaQuery().getFilterClauseQueue());

        ((RDBMSEntityReader) getReader()).setFilter(getKunderaQuery().getFilter());

        List<EnhanceEntity> ls = getReader().populateRelation(m, relationNames, isParent, client);
        // pass graph and list of enhanced entities and graph for association
        // population.
        return handleGraph(ls, graphs, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        log.debug("on start of fetching non associated entities");

        List<Object> result = null;

        ((RDBMSEntityReader) getReader()).setConditions(getKunderaQuery().getFilterClauseQueue());

        ((RDBMSEntityReader) getReader()).setFilter(getKunderaQuery().getFilter());

        try
        {
            if (MetadataUtils.useSecondryIndex(client.getPersistenceUnit()))
            {
                List<String> relations = new ArrayList<String>();
                List r = ((HibernateClient) client).find(
                        ((RDBMSEntityReader) getReader()).getSqlQueryFromJPA(m, relations, null), relations, m);
                result = new ArrayList<Object>(r.size());

                for (Object o : r)
                {
                    Class clazz = m.getEntityClazz();
                    if (!o.getClass().isAssignableFrom(m.getEntityClazz()))
                    {
                        o = ((Object[]) o)[0];
                    }
                    result.add(o);
                }
            }
            else
            {
                result = populateUsingLucene(m, client, result);
            }
        }
        catch (Exception e)
        {
            log.error("Error during query execution" + e.getMessage());
            throw new QueryHandlerException(e.getMessage());
        }

        if (result != null && !result.isEmpty())
        {
            persistenceDelegeator.store(result, m);
        }

        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        if (reader == null)
        {
            reader = new RDBMSEntityReader(getLuceneQueryFromJPAQuery(), getJPAQuery());
        }
        return reader;
    }

}
