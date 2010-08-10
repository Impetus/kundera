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
package com.impetus.kundera.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.PropertyIndex;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class LucandraIndexer.
 * 
 * @author animesh.kumar
 */
public class LucandraIndexer implements Indexer {

	/** log for this class. */
	private static Log log = LogFactory.getLog(ColumnFamilyDataAccessor.class);

	/** The INDEX_NAME. */
	private static String INDEX_NAME = "kundera-alpha";// is
														// persistent-unit-name

	/** The Constant UUID. */
	private static final long UUID = 6077004083174677888L;

	/** The Constant DELIMETER. */
	private static final String DELIMETER = "~";

	/** The Constant ENTITY_ID_FIELD. */
	public static final String ENTITY_ID_FIELD = UUID + ".entity.id";

	/** The Constant KUNDERA_ID_FIELD. */
	public static final String KUNDERA_ID_FIELD = UUID + ".kundera.id";

	/** The Constant ENTITY_INDEXNAME_FIELD. */
	public static final String ENTITY_INDEXNAME_FIELD = UUID
			+ ".entity.indexname";

	/** The Constant ENTITY_CLASS_FIELD. */
	public static final String ENTITY_CLASS_FIELD = UUID + ".entity.class";

	/** The Constant DEFAULT_SEARCHABLE_FIELD. */
	private static final String DEFAULT_SEARCHABLE_FIELD = UUID
			+ ".default_property";

	/** The client. */
	private CassandraClient client;

	/** The analyzer. */
	private Analyzer analyzer;

	/**
	 * Instantiates a new lucandra indexer.
	 * 
	 * @param client
	 *            the client
	 * @param analyzer
	 *            the analyzer
	 */
	public LucandraIndexer(CassandraClient client, Analyzer analyzer) {
		this.client = client;
		this.analyzer = analyzer;
	}

	/*
	 * @see
	 * com.impetus.kundera.index.Indexer#unindex(com.impetus.kundera.metadata
	 * .EntityMetadata, java.lang.String)
	 */
	@Override
	public final void unindex(EntityMetadata metadata, String id) {
		log.debug("Unindexing @Entity[" + metadata.getEntityClazz().getName()
				+ "] for key:" + id);
		try {
			getIndexWriter().deleteDocuments(
					new Term(KUNDERA_ID_FIELD, getKunderaId(metadata, id)));
		} catch (CorruptIndexException e) {
			throw new IndexingException(e.getMessage());
		} catch (IOException e) {
			throw new IndexingException(e.getMessage());
		}
	}

	/*
	 * @see
	 * com.impetus.kundera.index.Indexer#index(com.impetus.kundera.metadata.
	 * EntityMetadata, java.lang.Object)
	 */
	@Override
	public final void index(EntityMetadata metadata, Object object) {

		if (!metadata.isIndexable()) {
			return;
		}

		log.debug("Indexing @Entity[" + metadata.getEntityClazz().getName()
				+ "] " + object);

		String indexName = metadata.getIndexName();

		Document document = new Document();
		Field luceneField;

		// index row
		try {
			String id = PropertyAccessorHelper.getId(object, metadata);
			luceneField = new Field(ENTITY_ID_FIELD, id, // adding class
					// namespace
					Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);

			// index namespace for unique deletion
			luceneField = new Field(KUNDERA_ID_FIELD,
					getKunderaId(metadata, id), // adding
					// class
					// namespace
					Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);

			// index entity class
			luceneField = new Field(ENTITY_CLASS_FIELD, metadata
					.getEntityClazz().getCanonicalName(), Field.Store.YES,
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);

			// index index name
			luceneField = new Field(ENTITY_INDEXNAME_FIELD, metadata
					.getIndexName(), Field.Store.YES,
					Field.Index.NOT_ANALYZED_NO_NORMS);
			document.add(luceneField);

		} catch (PropertyAccessException e) {
			throw new IllegalArgumentException("Id could not be read.");
		}

		// now index all indexable properties
		for (PropertyIndex index : metadata.getIndexProperties()) {

			java.lang.reflect.Field property = index.getProperty();
			String propertyName = index.getName();

			try {
				String value = PropertyAccessorHelper.getString(object,
						property).toString();
				luceneField = new Field(getCannonicalPropertyName(indexName,
						propertyName), value, Field.Store.NO,
						Field.Index.ANALYZED);
				document.add(luceneField);
			} catch (PropertyAccessException e) {
				// TODO: do something with the exceptions
				// e.printStackTrace();
			}
		}

		// flush the indexes
		try {
			log.debug("Flushing to Lucandra: " + document);
			getIndexWriter().addDocument(document, analyzer);
		} catch (CorruptIndexException e) {
			throw new IndexingException(e.getMessage());
		} catch (IOException e) {
			throw new IndexingException(e.getMessage());
		}
	}

	// TODO: this is not the best implementation. need to improve!
	/* @see com.impetus.kundera.index.Indexer#search(java.lang.String, int, int) */
	@SuppressWarnings("deprecation")
	@Override
	public final List<String> search(String luceneQuery, int start, int count) {

		if (Constants.INVALID == count) {
			count = 100;
		}

		log.debug("Searhcing index with query[" + luceneQuery + "], start:"
				+ start + ", count:" + count);

		Set<String> entityIds = new HashSet<String>();

		IndexReader indexReader = null;
		try {
			indexReader = new IndexReader(INDEX_NAME, client
					.getCassandraClient());
		} catch (Exception e) {
			throw new IndexingException(e.getMessage());
		}
		IndexSearcher searcher = new IndexSearcher(indexReader);

		QueryParser qp = new QueryParser(Version.LUCENE_CURRENT,
				DEFAULT_SEARCHABLE_FIELD, analyzer);
		try {
			Query q = qp.parse(luceneQuery);
			TopDocs docs = searcher.search(q, count);

			for (ScoreDoc sc : docs.scoreDocs) {
				Document doc = searcher.doc(sc.doc);
				entityIds.add(doc.get(ENTITY_ID_FIELD));
			}
		} catch (ParseException e) {
			new IndexingException(e.getMessage());
		} catch (IOException e) {
			new IndexingException(e.getMessage());
		}

		log.debug("Result[" + entityIds + "]");
		return new ArrayList<String>(entityIds);
	}

	/**
	 * Gets the kundera id.
	 * 
	 * @param metadata
	 *            the metadata
	 * @param id
	 *            the id
	 * 
	 * @return the kundera id
	 */
	private String getKunderaId(EntityMetadata metadata, String id) {
		return metadata.getEntityClazz().getCanonicalName() + DELIMETER + id;
	}

	/**
	 * Gets the cannonical property name.
	 * 
	 * @param indexName
	 *            the index name
	 * @param propertyName
	 *            the property name
	 * 
	 * @return the cannonical property name
	 */
	private String getCannonicalPropertyName(String indexName,
			String propertyName) {
		return indexName + "." + propertyName;
	}

	// helper method to get Lucandra IndexWriter object
	/**
	 * Gets the index writer.
	 * 
	 * @return the index writer
	 */
	private lucandra.IndexWriter getIndexWriter() {
		try {
			return new lucandra.IndexWriter(INDEX_NAME, client
					.getCassandraClient());
		} catch (Exception e) {
			throw new IndexingException(e.getMessage());
		}
	}
}
