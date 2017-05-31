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

package com.impetus.client.mongodb.schemamanager;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.client.mongodb.config.MongoDBPropertyReader;
import com.impetus.client.mongodb.index.IndexType;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.IndexInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

/**
 * The Class MongoDBSchemaManager manages auto schema operation
 * {@code ScheamOperationType} for mongoDb database.
 * 
 * @author Kuldeep.kumar
 * 
 */
public class MongoDBSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    /** The m. */
    private MongoClient mongo;

    /** The db. */
    private DB db;

    /** The coll. */
    private DBCollection coll;

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(MongoDBSchemaManager.class);

    Logger mongoLogger = LoggerFactory.getLogger(com.mongodb.DB.class);

    public MongoDBSchemaManager(String clientFactory, Map<String, Object> externalProperties,
            final KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
    }

    /**
     * drop schema method drop the table
     */
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            for (TableInfo tableInfo : tableInfos)
            {
                if (tableInfo.getLobColumnInfo().isEmpty())
                {
                    coll = db.getCollection(tableInfo.getTableName());
                    coll.drop();
                    KunderaCoreUtils.printQuery("Drop collection:" + tableInfo.getTableName(), showQuery);
                }
                else
                {
                    coll = db.getCollection(tableInfo.getTableName() + MongoDBUtils.FILES);
                    coll.drop();
                    KunderaCoreUtils.printQuery("Drop collection:" + tableInfo.getTableName() + MongoDBUtils.FILES,
                            showQuery);
                    coll = db.getCollection(tableInfo.getTableName() + MongoDBUtils.CHUNKS);
                    coll.drop();
                    KunderaCoreUtils.printQuery("Drop collection:" + tableInfo.getTableName() + MongoDBUtils.CHUNKS,
                            showQuery);
                }
            }
        }
        db = null;
    }

    /**
     * create method creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create(List<TableInfo> tableInfos)
    {
        for (TableInfo tableInfo : tableInfos)
        {
            DBObject options = setCollectionProperties(tableInfo);
            DB db = mongo.getDB(databaseName);

            if (tableInfo.getLobColumnInfo().isEmpty())
            {
                if (db.collectionExists(tableInfo.getTableName()))
                {
                    db.getCollection(tableInfo.getTableName()).drop();

                    KunderaCoreUtils.printQuery("Drop existing collection: " + tableInfo.getTableName(), showQuery);
                }
                DBCollection collection = db.createCollection(tableInfo.getTableName(), options);
                KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName(), showQuery);

                boolean isCappedCollection = isCappedCollection(tableInfo);
                if (!isCappedCollection)
                {
                    createIndexes(tableInfo, collection);
                }
            }
            else
            {
                /*
                 * In GridFS we have 2 collections. TABLE_NAME.chunks for
                 * storing chunks of data and TABLE_NAME.MongoDBUtils.FILES for
                 * storing it's metadata.
                 */
                checkMultipleLobs(tableInfo);
                if (db.collectionExists(tableInfo.getTableName() + MongoDBUtils.FILES))
                {
                    db.getCollection(tableInfo.getTableName() + MongoDBUtils.FILES).drop();
                    KunderaCoreUtils.printQuery(
                            "Drop existing Grid FS Metadata collection: " + tableInfo.getTableName()
                                    + MongoDBUtils.FILES, showQuery);
                }

                if (db.collectionExists(tableInfo.getTableName() + MongoDBUtils.CHUNKS))
                {
                    db.getCollection(tableInfo.getTableName() + MongoDBUtils.CHUNKS).drop();
                    KunderaCoreUtils.printQuery("Drop existing Grid FS data collection: " + tableInfo.getTableName()
                            + MongoDBUtils.CHUNKS, showQuery);
                }
                coll = db.createCollection(tableInfo.getTableName() + MongoDBUtils.FILES, options);
                createUniqueIndexGFS(coll, tableInfo.getIdColumnName());
                KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName() + MongoDBUtils.FILES,
                        showQuery);
                db.createCollection(tableInfo.getTableName() + MongoDBUtils.CHUNKS, options);
                KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName() + MongoDBUtils.CHUNKS,
                        showQuery);

            }
        }
    }

    /**
     * create_drop method creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create_drop(List<TableInfo> tableInfos)
    {
        create(tableInfos);
    }

    /**
     * update method update schema and table for the list of tableInfos
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void update(List<TableInfo> tableInfos)
    {
        for (TableInfo tableInfo : tableInfos)
        {
            DBObject options = setCollectionProperties(tableInfo);
            DB db = mongo.getDB(databaseName);
            DBCollection collection = null;
            if (tableInfo.getLobColumnInfo().isEmpty())
            {
                if (!db.collectionExists(tableInfo.getTableName()))
                {
                    collection = db.createCollection(tableInfo.getTableName(), options);
                    KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName(), showQuery);
                }

                collection = collection != null ? collection : db.getCollection(tableInfo.getTableName());

                boolean isCappedCollection = isCappedCollection(tableInfo);
                if (!isCappedCollection)
                {
                    createIndexes(tableInfo, collection);
                }
            }
            else
            {
                checkMultipleLobs(tableInfo);
                if (!db.collectionExists(tableInfo.getTableName() + MongoDBUtils.FILES))
                {
                    coll = db.createCollection(tableInfo.getTableName() + MongoDBUtils.FILES, options);
                    createUniqueIndexGFS(coll, tableInfo.getIdColumnName());
                    KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName() + MongoDBUtils.FILES,
                            showQuery);
                }

                if (!db.collectionExists(tableInfo.getTableName() + MongoDBUtils.CHUNKS))
                {
                    db.createCollection(tableInfo.getTableName() + MongoDBUtils.CHUNKS, options);
                    KunderaCoreUtils.printQuery("Create collection: " + tableInfo.getTableName() + MongoDBUtils.CHUNKS,
                            showQuery);
                }
            }
        }
    }

    /**
     * validate method validate schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void validate(List<TableInfo> tableInfos)
    {
        db = mongo.getDB(databaseName);
        if (db == null)
        {
            logger.error("Database " + databaseName + "does not exist");
            throw new SchemaGenerationException("database " + databaseName + "does not exist", "mongoDb", databaseName);
        }
        else
        {
            for (TableInfo tableInfo : tableInfos)
            {
                if (tableInfo.getLobColumnInfo().isEmpty())
                {
                    if (!db.collectionExists(tableInfo.getTableName()))
                    {
                        logger.error("Collection " + tableInfo.getTableName() + "does not exist in db " + db.getName());
                        throw new SchemaGenerationException("Collection " + tableInfo.getTableName()
                                + " does not exist in db " + db.getName(), "mongoDb", databaseName,
                                tableInfo.getTableName());
                    }
                }
                else
                {
                    checkMultipleLobs(tableInfo);
                    if (!db.collectionExists(tableInfo.getTableName() + MongoDBUtils.FILES))
                    {
                        logger.error("Collection " + tableInfo.getTableName() + MongoDBUtils.FILES
                                + "does not exist in db " + db.getName());
                        throw new SchemaGenerationException("Collection " + tableInfo.getTableName()
                                + " does not exist in db " + db.getName(), "mongoDb", databaseName,
                                tableInfo.getTableName());
                    }
                    if (!db.collectionExists(tableInfo.getTableName() + MongoDBUtils.CHUNKS))
                    {
                        logger.error("Collection " + tableInfo.getTableName() + MongoDBUtils.CHUNKS
                                + "does not exist in db " + db.getName());
                        throw new SchemaGenerationException("Collection " + tableInfo.getTableName()
                                + " does not exist in db " + db.getName(), "mongoDb", databaseName,
                                tableInfo.getTableName());
                    }

                }
            }
        }
    }

    /**
     * initiate client method initiates the client.
     * 
     * @return boolean value ie client started or not.
     * 
     */
    protected boolean initiateClient()
    {
        for (String host : hosts)
        {
            if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
            {
                logger.error("Host or port should not be null / port should be numeric");
                throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
            }

            List<MongoCredential> credentials = MongoDBUtils.fetchCredentials(puMetadata.getProperties(),
                    externalProperties);
            ServerAddress addr;
            try
            {
                addr = new ServerAddress(host, Integer.parseInt(port));
            }
            catch (NumberFormatException ex)
            {
                throw new SchemaGenerationException(ex);
            }

            mongo = new MongoClient(addr, credentials);
            db = mongo.getDB(databaseName);
            return true;
        }
        return false;
    }

    /**
     * @param tableInfo
     * @return
     */
    private DBObject setCollectionProperties(TableInfo tableInfo)
    {
        boolean isCappedCollection = isCappedCollection(tableInfo);
        DBObject options = new BasicDBObject();
        if ((tableInfo.getLobColumnInfo().isEmpty() || tableInfo.getLobColumnInfo() == null) && isCappedCollection)
        {
            int collectionSize = MongoDBPropertyReader.msmd != null ? MongoDBPropertyReader.msmd.getCollectionSize(
                    databaseName, tableInfo.getTableName()) : 100000;
            int max = MongoDBPropertyReader.msmd != null ? MongoDBPropertyReader.msmd.getMaxSize(databaseName,
                    tableInfo.getTableName()) : 100;
            options.put(MongoDBConstants.CAPPED, isCappedCollection);
            options.put(MongoDBConstants.SIZE, collectionSize);
            options.put(MongoDBConstants.MAX, max);
        }
        return options;
    }

    /**
     * Checks whether the given table is a capped collection
     */
    protected boolean isCappedCollection(TableInfo tableInfo)
    {
        return MongoDBPropertyReader.msmd != null ? MongoDBPropertyReader.msmd.isCappedCollection(databaseName,
                tableInfo.getTableName()) : false;
    }

    /**
     * @param tableInfo
     * @param collection
     */
    private void createIndexes(TableInfo tableInfo, DBCollection collection)
    {
        // index normal column
        for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
        {
            if (columnInfo.isIndexable())
            {
                IndexInfo indexInfo = tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
                indexColumn(indexInfo, collection);
            }
        }

        // index embedded column.
        for (EmbeddedColumnInfo info : tableInfo.getEmbeddedColumnMetadatas())
        {
            for (ColumnInfo columnInfo : info.getColumns())
            {
                if (columnInfo.isIndexable())
                {
                    IndexInfo indexInfo = tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
                    indexEmbeddedColumn(indexInfo, info.getEmbeddedColumnName(), collection);
                }
            }
        }
    }

    private void indexColumn(IndexInfo indexInfo, DBCollection collection)
    {
        DBObject keys = new BasicDBObject();
        getIndexType(indexInfo.getIndexType(), keys, indexInfo.getColumnName());
        DBObject options = new BasicDBObject();
        if (indexInfo.getMinValue() != null)
        {
            options.put(MongoDBConstants.MIN, indexInfo.getMinValue());
        }
        if (indexInfo.getMaxValue() != null)
        {
            options.put(MongoDBConstants.MAX, indexInfo.getMaxValue());
        }

        if (indexInfo.getIndexType() != null && (indexInfo.getIndexType().toLowerCase()).equals("unique"))
        {
            options.put("unique", true);
        }
        collection.createIndex(keys, options);
        KunderaCoreUtils.printQuery("Create indexes on:" + keys, showQuery);
    }

    private void indexEmbeddedColumn(IndexInfo indexInfo, String embeddedColumnName, DBCollection collection)
    {
        DBObject keys = new BasicDBObject();
        getIndexType(indexInfo.getIndexType(), keys, embeddedColumnName + "." + indexInfo.getColumnName());
        DBObject options = new BasicDBObject();
        if (indexInfo.getMinValue() != null)
        {
            options.put(MongoDBConstants.MIN, indexInfo.getMinValue());
        }
        if (indexInfo.getMaxValue() != null)
        {
            options.put(MongoDBConstants.MAX, indexInfo.getMaxValue());
        }
        collection.createIndex(keys, options);
        KunderaCoreUtils.printQuery("Create indexes on:" + keys, showQuery);
    }

    /**
     * @param indexType
     * @param clazz
     * @return
     */
    private void getIndexType(String indexType, DBObject keys, String columnName)
    {
        // TODO validation for indexType and attribute type

        if (indexType != null)
        {
            if (indexType.equals(IndexType.ASC.name()))
            {
                keys.put(columnName, 1);
                return;
            }
            else if (indexType.equals(IndexType.DSC.name()))
            {
                keys.put(columnName, -1);
                return;
            }
            else if (indexType.equals(IndexType.GEO2D.name()))
            {
                keys.put(columnName, "2d");
                return;
            }
        }
        keys.put(columnName, 1);
        return;
    }

    private void checkMultipleLobs(TableInfo tableInfo)
    {
        if (tableInfo.getLobColumnInfo().size() > 1)
            throw new KunderaException("Multiple Lob fields in a single Entity are not supported in Kundera");
    }

    private void createUniqueIndexGFS(DBCollection coll, String id)
    {
        try
        {
            coll.createIndex(new BasicDBObject("metadata." + id, 1), new BasicDBObject("unique", true));
        }
        catch (MongoException ex)
        {
            throw new KunderaException("Error in creating unique indexes in " + coll.getFullName() + " collection on "
                    + id + " field");
        }
    }

    @Override
    public boolean validateEntity(Class clazz)
    {
        return true;
    }

}
