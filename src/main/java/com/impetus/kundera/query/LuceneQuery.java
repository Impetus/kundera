/*
 * Copyright (c) 2010-2011, Animesh Kumar
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
 * @author animesh.kumar
 */
public class LuceneQuery extends QueryImpl implements Query {

	/** the log used by this class. */
	private static Log log = LogFactory.getLog(MetadataManager.class);

	int maxResult = Constants.INVALID;
	String luceneQuery;

	public LuceneQuery(EntityManagerImpl em, String jpaQuery) {
		super(em, jpaQuery);
	}

	public void setLuceneQuery(String luceneQuery) {
		this.luceneQuery = luceneQuery;
	}

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

	@Override
	public Query setMaxResults(int maxResult) {
		this.maxResult = maxResult;
		return this;
	}

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
