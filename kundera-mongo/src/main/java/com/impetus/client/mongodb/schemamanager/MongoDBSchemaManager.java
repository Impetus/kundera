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

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
    private Mongo m;

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
     * update method update schema and table for the list of tableInfos
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void update(List<TableInfo> tableInfos)
    {
        // Do nothing as by default mongo handles it.
    }

    /**
     * create method creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create(List<TableInfo> tableInfos)
    {
        // Do nothing as by default mongo handles it.
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
     * validate method validate schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void validate(List<TableInfo> tableInfos)
    {
        // List<String> dbNames = m.getDatabaseNames();
        // boolean found = false;
        // for (String dbName : dbNames)
        // {
        // if (dbName.equalsIgnoreCase(databaseName))
        // {
        // found = true;
        // }
        // }
        // if (!found)
        // {
        // logger.error("database " + databaseName + "does not exist");
        // throw new SchemaGenerationException("mongoDb", databaseName);
        // }
        // else
        // {
        db = m.getDB(databaseName);
        // }

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
                            + "does not exist in db " + db.getName(), "mongoDb", databaseName, tableInfo.getTableName());
                }
            }
        }
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
            m = new Mongo(host, localport);
            db = m.getDB(puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_KEYSPACE));
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

    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return true;
    }
}
