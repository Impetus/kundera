/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.oraclenosql.schemamanager;

import java.util.List;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.table.StatementResult;
import oracle.kv.table.TableAPI;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class OracleNoSQLSchemaManager.
 * 
 * @author devender.yadav
 * 
 */
public class OracleNoSQLSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(OracleNoSQLSchemaManager.class);

    /** The kv store. */
    private KVStore kvStore;

    /** The table api. */
    private TableAPI tableAPI;

    /**
     * Instantiates a new oracle no sql schema manager.
     * 
     * @param clientFactory
     *            the client factory
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public OracleNoSQLSchemaManager(String clientFactory, Map<String, Object> externalProperties,
            KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, externalProperties, kunderaMetadata);
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
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#exportSchema
     * (java.lang.String, java.util.List)
     */
    @Override
    public void exportSchema(String persistenceUnit, List<TableInfo> puToSchemaCol)
    {
        super.exportSchema(persistenceUnit, puToSchemaCol);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema()
    {

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
                kvStore = KVStoreFactory.getStore(new KVStoreConfig(databaseName, host + ":" + port));
            }
            catch (FaultException e)
            {
                logger.error("Unable to get KVStore. Caused by ", e);
                throw new KunderaException("Unable to get KVStore. Caused by ", e);
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
        tableAPI = kvStore.getTableAPI();
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
        tableAPI = kvStore.getTableAPI();

        StatementResult result = null;
        String statement = null;

        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                tableAPI.executeSync("DROP TABLE IF EXISTS " + tableInfo.getTableName());
                
                StringBuilder builder = new StringBuilder();
                builder.append("CREATE TABLE ");
                builder.append(tableInfo.getTableName());
                builder.append("(");

                builder.append(tableInfo.getIdColumnName());
                builder.append(" ");
                builder.append(tableInfo.getTableIdType().getSimpleName());
                builder.append(",");

                for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                {
                    builder.append(columnInfo.getColumnName());
                    builder.append(" ");
                    builder.append(columnInfo.getType().getSimpleName());
                    builder.append(",");
                }

                builder.append("PRIMARY KEY");
                builder.append("(");
                builder.append(tableInfo.getIdColumnName());
                builder.append(")");
                builder.append(")");

                statement = builder.toString();

                result = tableAPI.executeSync(statement);
                
                if (!result.isSuccessful())
                {
                    throw new SchemaGenerationException("unable to create tables");
                }

            }
            catch (IllegalArgumentException e)
            {
                logger.error("invalid Statement. Caused By: ", e);
                throw new SchemaGenerationException(e);
            }
            catch (FaultException e)
            {
                logger.error("Statement couldn't be executed. Caused By: ", e);
                throw new SchemaGenerationException(e);
            }
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

    }

}
