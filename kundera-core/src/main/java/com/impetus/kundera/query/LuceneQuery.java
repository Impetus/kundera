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
package com.impetus.kundera.query;

import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;

/**
 * The Class LuceneQuery.
 * 
 * @author animesh.kumar
 */
public class LuceneQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetadataBuilder.class);

    /** The max result. */
    int maxResult = Constants.INVALID;

    /** The lucene query. */
    String luceneQuery;

    /**
     * Instantiates a new lucene query.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param kunderaQuery
     *            the kundera query
     * @param pd
     *            the pd
     * @param persistenceUnits
     *            the persistence units
     */
    public LuceneQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator pd, String... persistenceUnits)
    {
        super(jpaQuery, pd, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    /**
     * Sets the lucene query.
     * 
     * @param luceneQuery
     *            the new lucene query
     */
    public void setLuceneQuery(String luceneQuery)
    {
        this.luceneQuery = luceneQuery;
    }

    // @see com.impetus.kundera.query.QueryImpl#getResultList()
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getResultList()
     */
    @Override
    public List<?> getResultList()
    {
        log.debug("JPA Query: " + query);

        // get luence query
        String q = luceneQuery;
        if (null == q)
        {
            q = getLuceneQueryFromJPAQuery();
        }

        log.debug("Lucene Query: " + q);

        EntityMetadata m = kunderaQuery.getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);
        Map<String, String> searchFilter = client.getIndexManager().search(q, -1, maxResult);

        if (kunderaQuery.isAliasOnly())
        {
            String[] primaryKeys = searchFilter.values().toArray(new String[] {});
            return persistenceDelegeator.find(m.getEntityClazz(), primaryKeys);
        }
        else
        {
            return persistenceDelegeator.find(m.getEntityClazz(), searchFilter);

        }
       

    }

    // @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        this.maxResult = maxResult;
        return this;
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
        throw new UnsupportedOperationException("Method not supported for default indexing");
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
        throw new UnsupportedOperationException("Method not supported for default indexing");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        throw new UnsupportedOperationException("Method not supported for default indexing");
    }
}
