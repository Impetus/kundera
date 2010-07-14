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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lucandra.IndexReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.Constants;
import com.impetus.kundera.db.accessor.ColumnFamilyDataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.PropertyIndex;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * @author animesh.kumar
 *
 */
public class LucandraIndexer implements Indexer {
	
	/** log for this class */
	private static Log log = LogFactory.getLog(ColumnFamilyDataAccessor.class);
	
	private static String INDEX_NAME; // is persistent-unit-name
	
	private static final long   UUID 						= 6077004083174677888L;
	private static final String DELIMETER 					= "~";
	public  static final String ENTITY_ID_FIELD 			= UUID + ".entity.id";
	public  static final String KUNDERA_ID_FIELD 			= UUID + ".kundera.id";
	public  static final String ENTITY_INDEXNAME_FIELD 		= UUID + ".entity.indexname";
	public  static final String ENTITY_CLASS_FIELD 			= UUID + ".entity.class";
	private static final String DEFAULT_SEARCHABLE_FIELD 	= UUID + ".default_property";
	
	private CassandraClient client;
	private lucandra.IndexWriter indexWriter;
	Analyzer analyzer;
	
	public LucandraIndexer(EntityManagerImpl em, Analyzer analyzer) throws Exception {
		INDEX_NAME = em.getPersistenceUnitName();
		this.client = em.getClient();
		this.analyzer = analyzer;
		this.indexWriter = new lucandra.IndexWriter(INDEX_NAME, client.getCassandraClient());
	}
	
	@Override
	public void unindex (EntityMetadata metadata, String id) {
		log.debug("Unindexing @Entity[" + metadata.getEntityClazz().getName() + "] for key:" + id);
		try {
			indexWriter.deleteDocuments(new Term(KUNDERA_ID_FIELD, getKunderaId(metadata, id)));
		} catch (CorruptIndexException e) {
			throw new IndexingException(e.getMessage());
		} catch (IOException e) {
			throw new IndexingException(e.getMessage());
		}
	}
	
	@Override
	public void index (EntityMetadata metadata, Object object) {
		
		if (!metadata.isIndexable()) {
			return;
		}
		
		log.debug("Indexing @Entity[" + metadata.getEntityClazz().getName() + "] " + object);
		
		String indexName = metadata.getIndexName();
		
		Document document = new Document();
		Field luceneField;
		
		// index row
		try {
			String id = PropertyAccessorFactory.getStringProperty(object, metadata.getIdProperty());
			luceneField = new Field(ENTITY_ID_FIELD, 
					id, // adding class namespace
					Field.Store.YES, 
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);
			
			// index namespace for unique deletion
			luceneField = new Field(KUNDERA_ID_FIELD, 
					getKunderaId(metadata, id), // adding class namespace
					Field.Store.YES, 
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);
			
			// index entity class
			luceneField = new Field(ENTITY_CLASS_FIELD, 
					metadata.getEntityClazz().getCanonicalName(), 
					Field.Store.YES, 
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);
			
			// index index name
			luceneField = new Field(ENTITY_INDEXNAME_FIELD, 
					metadata.getIndexName(), 
					Field.Store.YES, 
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);

		} catch (PropertyAccessException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Id could not be read.");
		}
		
		// now index all indexable properties
		for (PropertyIndex index : metadata.getIndexProperties()) {
			
			java.lang.reflect.Field property = index.getProperty();
			String propertyName = index.getName();
			try {
				
				Map<String, ?> map = PropertyAccessorFactory.getPropertyAccessor(property).readAsObject(object, property, propertyName);
				
				for (Map.Entry<String, ?> entry : map.entrySet()) {
					luceneField = new Field(getCannonicalPropertyName(indexName, propertyName), 
							entry.getValue().toString(), 
							Field.Store.NO, 
							Field.Index.ANALYZED);
					document.add(luceneField);
				}
				
			} catch (PropertyAccessException e) {
				// TODO: do something with the exceptions
				//e.printStackTrace();
			}
		}

		// flush the indexes
		try {
			log.debug("Flushing to Lucandra: " + document);
			indexWriter.addDocument(document, analyzer);
		} catch (CorruptIndexException e) {
			throw new IndexingException(e.getMessage());
		} catch (IOException e) {
			throw new IndexingException(e.getMessage());
		}
	}

	// TODO: this is not the best implementation. need to improve!
	@SuppressWarnings("deprecation")
	@Override
	public List<String> search (String luceneQuery, int start, int count) {
		
		if (Constants.INVALID == count) {
			count = 100;
		}
		
		log.debug("Searhcing index with query[" + luceneQuery + "], start:" + start + ", count:" + count);
		
		Set<String> entityIds = new HashSet<String>();
		
        IndexReader indexReader = null;
		try {
			indexReader = new IndexReader(INDEX_NAME, client.getCassandraClient());
		} catch (Exception e) {
			throw new IndexingException(e.getMessage());
		}
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, DEFAULT_SEARCHABLE_FIELD, analyzer);
		try {
			Query q = qp.parse(luceneQuery);
			TopDocs docs = searcher.search(q, count);
			
			for (ScoreDoc sc : docs.scoreDocs) {
				Document doc = searcher.doc(sc.doc);
				entityIds.add (doc.get(ENTITY_ID_FIELD));
			}
		} catch (ParseException e) {
			new IndexingException(e.getMessage());
		} catch (IOException e) {
			new IndexingException(e.getMessage());
		}
		
		log.debug("Result[" + entityIds + "]");
		return new ArrayList<String>(entityIds);
	}
	
	private String getKunderaId (EntityMetadata metadata, String id) {
		return metadata.getEntityClazz().getCanonicalName() + DELIMETER + id;
	}

	private String getCannonicalPropertyName (String indexName, String propertyName) {
		return indexName + "." + propertyName;
	}

}
