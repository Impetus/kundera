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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.commons.lang.NotImplementedException;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.MetadataManager;

/**
 * The Class QueryImpl.
 * 
 * @author animesh.kumar
 */
public abstract class QueryImpl extends KunderaQuery implements Query {

	/** The query. */
	protected String query;

	/**
	 * Instantiates a new query impl.
	 * 
	 * @param em
	 *            the em
	 * @param metadataManager
	 *            the metadata manager
	 * @param query
	 *            the query
	 */
	public QueryImpl(EntityManagerImpl em, MetadataManager metadataManager,
			String query) {
		super(em, metadataManager);
		this.query = query;
		parse();
	}

	/**
	 * Gets the jPA query.
	 * 
	 * @return the jPA query
	 */
	public String getJPAQuery() {
		return query;
	}

	/**
	 * Parses the.
	 */
	private void parse() {
		KunderaQueryParser parser = new KunderaQueryParser(this, query);
		parser.parse();
		postParsingInit();
	}

	/* @see javax.persistence.Query#executeUpdate() */
	@Override
	public int executeUpdate() {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.Query#getResultList() */
	@Override
	public List<?> getResultList() {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.Query#getSingleResult() */
	@Override
	public Object getSingleResult() {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.Query#setFirstResult(int) */
	@Override
	public Query setFirstResult(int startPosition) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
	 */
	@Override
	public Query setFlushMode(FlushModeType flushMode) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.Query#setHint(java.lang.String, java.lang.Object) */
	@Override
	public Query setHint(String hintName, Object value) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.Query#setMaxResults(int) */
	@Override
	public Query setMaxResults(int maxResult) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.Query#setParameter(java.lang.String,
	 * java.lang.Object)
	 */
	@Override
	public Query setParameter(String name, Object value) {
		setParameter(name, value.toString());
		return this;
	}

	/* @see javax.persistence.Query#setParameter(int, java.lang.Object) */
	@Override
	public Query setParameter(int position, Object value) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.Query#setParameter(java.lang.String,
	 * java.util.Date, javax.persistence.TemporalType)
	 */
	@Override
	public Query setParameter(String name, Date value, TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.Query#setParameter(java.lang.String,
	 * java.util.Calendar, javax.persistence.TemporalType)
	 */
	@Override
	public Query setParameter(String name, Calendar value,
			TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.Query#setParameter(int, java.util.Date,
	 * javax.persistence.TemporalType)
	 */
	@Override
	public Query setParameter(int position, Date value,
			TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.Query#setParameter(int, java.util.Calendar,
	 * javax.persistence.TemporalType)
	 */
	@Override
	public Query setParameter(int position, Calendar value,
			TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

}
