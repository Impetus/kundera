/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.api.Document;
import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.metadata.EntityMetadata;

/**
 * Metadata processor for Document 
 * @author amresh.singh
 */
public class DocumentProcessor extends AbstractEntityFieldProcessor {

	/** The Constant log. */
	private static final Log LOG = LogFactory.getLog(DocumentProcessor.class);

	private EntityManagerFactoryImpl em;

	/**
	 * Instantiates a new document processor. 
	 * @param em the em
	 */
	public DocumentProcessor(EntityManagerFactory em) {
		this.em = (EntityManagerFactoryImpl) em;
	}
	
	
	@Override
	public void process(Class<?> clazz, EntityMetadata metadata) {
		if (!clazz.isAnnotationPresent(Document.class)) {
			return;
		}

		LOG.debug("Processing @Entity(" + clazz.getName() + ") for Document.");

		metadata.setType(EntityMetadata.Type.DOCUMENT);

		Document coll = clazz.getAnnotation(Document.class);

		// set columnFamily
		//Name of collection for document based datastore
		metadata.setColumnFamilyName(coll.name());	

		// set keyspace
		//DB name for document based datastore
		String keyspace = coll.db().length() != 0 ? coll.db() : em
				.getKeyspace();
		metadata.setKeyspaceName(keyspace);

		// scan for fields
		for (Field f : clazz.getDeclaredFields()) {

			// if @Id
			if (f.isAnnotationPresent(Id.class)) {
				LOG.debug(f.getName() + " => Id");
				metadata.setIdProperty(f);
				populateIdAccessorMethods(metadata, clazz, f);
				populateIdColumn(metadata, clazz, f);
			}

			// if any valid JPA annotation?
			else {
				String name = getValidJPAColumnName(clazz, f);
				if (null != name) {
					metadata.addColumn(name, metadata.new Column(name, f));
				}
			}
		}	
	}	

}
