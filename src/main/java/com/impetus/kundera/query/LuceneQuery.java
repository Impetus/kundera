/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.query;

import java.util.List;
import java.util.Queue;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.index.LucandraIndexer;
import com.impetus.kundera.metadata.MetadataManager;

/**
 * The Class LuceneQuery.
 * 
 * @author animesh.kumar
 */
public class LuceneQuery extends QueryImpl implements Query {

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetadataManager.class);

    /** The max result. */
    int maxResult = Constants.INVALID;

    /** The lucene query. */
    String luceneQuery;

    /**
     * Instantiates a new lucene query.
     * 
     * @param em
     *            the em
     * @param jpaQuery
     *            the jpa query
     */
    public LuceneQuery(EntityManagerImpl em, String jpaQuery) {
        super(em, jpaQuery);
    }

    /**
     * Sets the lucene query.
     * 
     * @param luceneQuery
     *            the new lucene query
     */
    public void setLuceneQuery(String luceneQuery) {
        this.luceneQuery = luceneQuery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getResultList()
     */
    @Override
    public List<?> getResultList() {
        // get luence query
        String q = luceneQuery;
        if (null == q) {
            q = getLuceneQueryFromJPAQuery();
        }

        log.debug("Lucene Query: " + q);

        // get entity ids from lucene index
        List<String> entityIds = cem.getIndexManager().search(q, maxResult);

        // lookup for corresponding entity classes
        return cem.find(getEntityClass(), entityIds.toArray());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult) {
        this.maxResult = maxResult;
        return this;
    }

    /**
     * Gets the lucene query from jpa query.
     * 
     * @return the lucene query from jpa query
     */
    private String getLuceneQueryFromJPAQuery() {
        StringBuffer sb = new StringBuffer();

        Queue filterClauseQueue = getFilterClauseQueue();

        for (Object object : getFilterClauseQueue()) {
            if (object instanceof FilterClause) {
                FilterClause filter = (FilterClause) object;
                sb.append("+");
                // property
                sb.append(filter.getProperty());

                // joiner
                String appender = "";
                if (filter.getCondition().equals("=")) {
                    sb.append(":");
                } else if (filter.getCondition().equalsIgnoreCase("like")) {
                    sb.append(":");
                    appender = "*";
                }

                // value
                sb.append(filter.getValue());
                sb.append(appender);
            } else {
                sb.append(" " + object + " ");
            }
        }

        // add Entity_CLASS field too.
        if (sb.length() > 0) {
            sb.append(" AND ");
        }
        sb.append("+");
        sb.append(LucandraIndexer.ENTITY_CLASS_FIELD);
        sb.append(":");
        sb.append(getEntityClass().getName());

        return sb.toString();
    }
}
