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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.commons.lang.NotImplementedException;

import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * @author animesh.kumar
 * 
 */
public abstract class QueryImpl extends KunderaQuery implements Query {

	protected EntityManagerImpl cem;
	protected String query;
	
	public QueryImpl(EntityManagerImpl cem, String query) {
		super (cem);
		this.cem = cem;
		this.query = query;
		
		parse();
	}
	
	protected void parse () {
		KunderaQueryParser parser = new KunderaQueryParser(this, query);
		parser.parse();
		postParsingInit();
	}
	
	@Override
	public int executeUpdate() {
		throw new NotImplementedException("TODO");
	}

	@Override
	public List<?> getResultList() {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Object getSingleResult() {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setFirstResult(int startPosition) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setFlushMode(FlushModeType flushMode) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setHint(String hintName, Object value) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setMaxResults(int maxResult) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setParameter(String name, Object value) {
		setParameter(name, value.toString());
		return this;
	}

	@Override
	public Query setParameter(int position, Object value) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setParameter(String name, Date value, TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setParameter(String name, Calendar value, TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setParameter(int position, Date value, TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query setParameter(int position, Calendar value, TemporalType temporalType) {
		throw new NotImplementedException("TODO");
	}

}
