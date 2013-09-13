/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * The Class LuceneQuery.
 * 
 * @author animesh.kumar
 */
public class LuceneQuery extends QueryImpl
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(MetadataBuilder.class);

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
    public LuceneQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator pd)
    {
        super(jpaQuery, pd);
        this.kunderaQuery = kunderaQuery;
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
        if (log.isDebugEnabled())
            log.debug("JPA Query: " + query);

        // get luence query
        String q = luceneQuery;
        if (null == q)
        {
            q = getLuceneQueryFromJPAQuery();
        }

        if (log.isDebugEnabled())
            log.debug("Lucene Query: " + q);

        EntityMetadata m = kunderaQuery.getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);

        handlePostEvent(m);
        Map<String, Object> searchFilter = client.getIndexManager().search(m.getEntityClazz(),q, -1, maxResult);

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
        throw new UnsupportedOperationException("Method not supported for Lucene indexing");
    }

    @Override
    protected EntityReader getReader()
    {
        throw new UnsupportedOperationException("Method not supported for Lucene indexing");
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        throw new UnsupportedOperationException("Method not supported for Lucene indexing");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
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
