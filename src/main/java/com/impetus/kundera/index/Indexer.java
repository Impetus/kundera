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
package com.impetus.kundera.index;

import java.util.List;

import com.impetus.kundera.metadata.EntityMetadata;

/**
 * Interface to define the behavior of an Indexer.
 * 
 * @author animesh.kumar
 *
 */
public interface Indexer {

	/**
	 * Unindexed an entity with key:id
	 * 
	 * @param metadata
	 * @param id
	 */
	void unindex(EntityMetadata metadata, String id);

	/**
	 * Indexes and object
	 * 
	 * @param metadata
	 * @param object
	 */
	void index(EntityMetadata metadata, Object object);

	/**
	 * Searches for an object. Note that the "query" must be in Indexer specified form.
	 * 
	 * @param luceneQuery
	 * @param start
	 * @param end
	 * @param count
	 * @return
	 */
	List<String> search (String query, int start, int count);

}
