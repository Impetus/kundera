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

import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.client.mongodb.config.MongoDBPropertyReader;
import com.impetus.client.mongodb.index.IndexType;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

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
    private Mongo mongo;

    /** The db. */
    private DB db;

    /** The coll. */
    private DBCollection coll;

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(MongoDBSchemaManager.class);

    public MongoDBSchemaManager(String clientFactory)
    {
        super(clientFactory);
    }

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema()
    {
        super.exportSchema();
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
                coll = db.getCollection(tableInfo.getTableName());
                coll.drop();
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
            getDBName();
            DB db = mongo.getDB(databaseName);
            if (db.collectionExists(tableInfo.getTableName()))
            {
                db.getCollection(tableInfo.getTableName()).drop();
            }
            DBCollection collection = db.createCollection(tableInfo.getTableName(), options);

            if (tableInfo.isIndexable())
            {
                createIndexes(tableInfo, collection);
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
            getDBName();
            DB db = mongo.getDB(databaseName);
            DBCollection collection = null;
            if (!db.collectionExists(tableInfo.getTableName()))
            {
                collection = db.createCollection(tableInfo.getTableName(), options);
            }
            collection = collection != null ? collection : db.getCollection(tableInfo.getTableName());
            createIndexes(tableInfo, collection);
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
                if (!db.collectionExists(tableInfo.getTableName()))
                {
                    logger.error("Collection " + tableInfo.getTableName() + "does not exist in db " + db.getName());
                    throw new SchemaGenerationException("Collection " + tableInfo.getTableName()
                            + " does not exist in db " + db.getName(), "mongoDb", databaseName,
                            tableInfo.getTableName());
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

        int localport = Integer.parseInt(port);
        try
        {
            mongo = new Mongo(host, localport);
            db = mongo.getDB(puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_KEYSPACE));
        }
        catch (UnknownHostException e)
        {
            logger.error("Database host cannot be resolved, Caused by" + e.getMessage());
            throw new SchemaGenerationException(e, "mongoDb");
        }
        catch (MongoException e)
        {

            throw new SchemaGenerationException(e);
        }

        return true;
    }

    /**
     * @param tableInfo
     * @return
     */
    private DBObject setCollectionProperties(TableInfo tableInfo)
    {
        boolean isCappedCollection = MongoDBPropertyReader.msmd != null ? MongoDBPropertyReader.msmd
                .isCappedCollection(databaseName, tableInfo.getTableName()) : false;
        DBObject options = new BasicDBObject();
        if (isCappedCollection)
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
     * @param tableInfo
     * @param collection
     */
    private void createIndexes(TableInfo tableInfo, DBCollection collection)
    {
        for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
        {
            if (columnInfo.isIndexable())
            {
                DBObject keys = new BasicDBObject();
                getIndexType(columnInfo.getIndexType(), keys, columnInfo.getColumnName());
                DBObject options = new BasicDBObject();
                if (columnInfo.getMinValue() != null)
                {
                    options.put("min", columnInfo.getMinValue());
                }
                if (columnInfo.getMaxValue() != null)
                {
                    options.put("max", columnInfo.getMaxValue());
                }
                collection.ensureIndex(keys, options);
            }
        }
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

    /**
     * 
     */
    private void getDBName()
    {
        List<String> dbs = mongo.getDatabaseNames();
        for (String dbName : dbs)
        {
            if (dbName.equalsIgnoreCase(databaseName))
            {
                databaseName = dbName;
                break;
            }
        }
    }

    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return true;
    }
   
}
