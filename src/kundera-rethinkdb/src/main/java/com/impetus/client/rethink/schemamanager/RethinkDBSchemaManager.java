/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.rethink.schemamanager;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

/**
 * The Class RethinkDBSchemaManager.
 * 
 * @author karthikp.manchala
 */
public class RethinkDBSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(RethinkDBSchemaManager.class);

    /** The connection. */
    private Connection connection;

    /** The Constant r. */
    private static final RethinkDB r = RethinkDB.r;

    /**
     * Instantiates a new rethink db schema manager.
     * 
     * @param clientFactory
     *            the client factory
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public RethinkDBSchemaManager(String clientFactory, Map<String, Object> externalProperties,
            KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema()
    {
        try
        {
            if (operation != null && ("create-drop").equalsIgnoreCase(operation))
            {
                r.dbDrop(databaseName);
            }
        }
        catch (Exception e)
        {
            logger.error("Error while dropping schema", e);
            throw new SchemaGenerationException(e, "RethinkDB");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.SchemaManager#validateEntity
     * (java.lang.Class)
     */
    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#initiateClient
     * ()
     */
    @Override
    protected boolean initiateClient()
    {
        for (String host : hosts)
        {
            if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
            {
                logger.error("Host or port should not be null / port should be numeric");
                throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
            }
            try
            {
                connection = r.connection().hostname(host).port(Integer.parseInt(port)).connect();
            }
            catch (Exception e)
            {
                logger.error("Database host/port cannot be resolved, Caused by: " + e.getMessage());
                throw new SchemaGenerationException("Database host/port cannot be resolved, Caused by: " + e.getMessage());
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#validate
     * (java.util.List)
     */
    @Override
    protected void validate(List<TableInfo> tableInfos)
    {
        List listTables = r.db(databaseName).tableList().run(connection);

        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                if (!listTables.contains(tableInfo.getTableName()))
                {
                    throw new SchemaGenerationException("Table: " + tableInfo.getTableName() + " does not exist ");
                }
            }
            catch (Exception e)
            {
                logger.error("Error while validating tables, Caused by: " + e.getMessage());
                throw new KunderaException("Error while validating tables, Caused by: " + e.getMessage());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#update
     * (java.util.List)
     */
    @Override
    protected void update(List<TableInfo> tableInfos)
    {
        List listTables = r.db(databaseName).tableList().run(connection);

        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                if (!listTables.contains(tableInfo.getTableName()))
                {
                    r.db(databaseName).tableCreate(tableInfo.getTableName()).run(connection);
                }
            }
            catch (Exception e)
            {
                logger.error("Error while updating tables, Caused by: " + e.getMessage());
                throw new KunderaException("Error while updating tables, Caused by: " + e.getMessage());
            }

        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create
     * (java.util.List)
     */
    @Override
    protected void create(List<TableInfo> tableInfos)
    {
        List dbList = r.dbList().run(connection);
        
        if(!dbList.contains(databaseName)){
            r.dbCreate(databaseName).run(connection);
        }
        
        List listTables = r.db(databaseName).tableList().run(connection);

        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                if (listTables.contains(tableInfo.getTableName()))
                {
                    r.db(databaseName).tableDrop(tableInfo.getTableName()).run(connection);
                }
            }
            catch (Exception e)
            {
                logger.error("Cannot check table existence for table " + tableInfo.getTableName() + ". Caused By: " + e);
                throw new KunderaException("Cannot check table existence for table " + tableInfo.getTableName()
                        + ". Caused By: " + e);
            }
            r.db(databaseName).tableCreate(tableInfo.getTableName()).run(connection);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create_drop
     * (java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos)
    {
        create(tableInfos);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * exportSchema(java.lang.String, java.util.List)
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
    }

}
