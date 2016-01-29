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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.Embeddable;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.PasswordCredentials;
import oracle.kv.table.StatementResult;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.IndexInfo;
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

    private Properties securityProps = new Properties();

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
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * exportSchema (java.lang.String, java.util.List)
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
        // dropping all the tables in the Oracle NoSQL store
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            for (TableInfo tableInfo : tableInfos)
            {
                try
                {
                    StatementResult result = tableAPI.executeSync("DROP TABLE IF EXISTS " + tableInfo.getTableName());
                    if (!result.isSuccessful())
                    {
                        throw new SchemaGenerationException("Unable to DROP TABLE " + tableInfo.getTableName());
                    }
                }
                catch (IllegalArgumentException e)
                {
                    logger.error("Invalid DROP TABLE Statement. Caused By: ", e);
                    throw new SchemaGenerationException(e, "Invalid DROP TABLE Statement. Caused By: ");
                }
                catch (FaultException e)
                {
                    logger.error("DROP TABLE Statement couldn't be executed. Caused By: ", e);
                    throw new SchemaGenerationException(e, "Invalid DROP TABLE Statement is executed. Caused By: ");
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * initiateClient ()
     */
    @Override
    protected boolean initiateClient()
    {
        String[] hostsList = new String[hosts.length];
        int count = 0;
        for (String host : hosts)
        {
            if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
            {
                logger.error("Host or port should not be null / port should be numeric");
                throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
            }
            hostsList[count] = host + ":" + port;
            count++;
        }
        KVStoreConfig kconfig = new KVStoreConfig(databaseName, hostsList);
        if (!securityProps.isEmpty())
        {

            kconfig.setSecurityProperties(securityProps);

        }

        try
        {
            if (userName != null && password != null)
            {
                kvStore = KVStoreFactory.getStore(kconfig, new PasswordCredentials(userName, password.toCharArray()),
                        null);
            }
            else
            {
                kvStore = KVStoreFactory.getStore(kconfig);
            }

            tableAPI = kvStore.getTableAPI();
        }
        catch (FaultException e)
        {
            logger.error("Unable to get KVStore. Caused by ", e);
            throw new KunderaException("Unable to get KVStore. Caused by ", e);
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
        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                if (tableAPI.getTable(tableInfo.getTableName()) == null)
                {
                    logger.error("No table found for " + tableInfo.getTableName());
                    throw new SchemaGenerationException("No table found for " + tableInfo.getTableName());
                }
            }
            catch (FaultException e)
            {
                logger.error("Error while getting table " + tableInfo.getTableName() + ". Caused By: ", e);
                throw new SchemaGenerationException(e, "Error while getting table " + tableInfo.getTableName()
                        + ". Caused By: ");
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
        StatementResult result = null;
        String statement = null;
        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                Table table = tableAPI.getTable(tableInfo.getTableName());
                if (table == null)
                {
                    statement = buildCreateDDLQuery(tableInfo);
                    result = tableAPI.executeSync(statement);
                    if (!result.isSuccessful())
                    {
                        throw new SchemaGenerationException("Unable to CREATE TABLE " + tableInfo.getTableName());
                    }
                }
                else
                {
                    List<ColumnInfo> columnInfos = tableInfo.getColumnMetadatas();
                    Map<String, String> newColumns = new HashMap<String, String>();
                    for (ColumnInfo column : columnInfos)
                    {
                        if (table.getField(column.getColumnName()) == null)
                        {
                            newColumns.put(column.getColumnName(), column.getType().getSimpleName());
                        }
                    }

                    List<EmbeddedColumnInfo> embeddedColumnInfos = tableInfo.getEmbeddedColumnMetadatas();
                    for (EmbeddedColumnInfo embeddedColumnInfo : embeddedColumnInfos)
                    {
                        for (ColumnInfo column : embeddedColumnInfo.getColumns())
                        {
                            if (table.getField(column.getColumnName()) == null)
                            {
                                newColumns.put(column.getColumnName(), column.getType().getSimpleName());
                            }
                        }
                    }
                    if (!newColumns.isEmpty())
                    {
                        statement = buildAlterDDLQuery(tableInfo, newColumns);
                        result = tableAPI.executeSync(statement);

                        if (!result.isSuccessful())
                        {
                            throw new SchemaGenerationException("Unable to ALTER TABLE " + tableInfo.getTableName());
                        }
                    }
                }
                createIndexOnTable(tableInfo);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Invalid Statement. Caused By: ", e);
                throw new SchemaGenerationException(e, "Invalid Statement. Caused By: ");
            }
            catch (FaultException e)
            {
                logger.error("Statement couldn't be executed. Caused By: ", e);
                throw new SchemaGenerationException(e, "Statement couldn't be executed. Caused By: ");
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
        StatementResult result = null;
        String statement = null;

        for (TableInfo tableInfo : tableInfos)
        {
            try
            {
                result = tableAPI.executeSync("DROP TABLE IF EXISTS " + tableInfo.getTableName());
                if (!result.isSuccessful())
                {
                    throw new SchemaGenerationException("Unable to DROP TABLE " + tableInfo.getTableName());
                }
                statement = buildCreateDDLQuery(tableInfo);
                result = tableAPI.executeSync(statement);
                if (!result.isSuccessful())
                {
                    throw new SchemaGenerationException("Unable to CREATE TABLE " + tableInfo.getTableName());
                }
                createIndexOnTable(tableInfo);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Invalid Statement. Caused By: ", e);
                throw new SchemaGenerationException(e, "Invalid Statement. Caused By: ");
            }
            catch (FaultException e)
            {
                logger.error("Statement couldn't be executed. Caused By: ", e);
                throw new SchemaGenerationException(e, "Statement couldn't be executed. Caused By: ");
            }
        }
    }

    /**
     * Creates the index on table.
     * 
     * @param tableInfo
     *            the table info
     */
    private void createIndexOnTable(TableInfo tableInfo)
    {

        List<IndexInfo> indexColumns = tableInfo.getColumnsToBeIndexed();
        for (IndexInfo indexInfo : indexColumns)
        {
            if (indexInfo.getIndexType() != null && indexInfo.getIndexType().toLowerCase().equals(Constants.COMPOSITE))
            {
                String[] columnNames = indexInfo.getColumnName().split(Constants.COMMA);
                createIndex(tableInfo.getTableName(), indexInfo.getIndexName(), columnNames);
            }
            else
            {
                createIndex(tableInfo.getTableName(), indexInfo.getIndexName(), indexInfo.getColumnName());
            }
        }
    }

    /**
     * Creates the index.
     * 
     * @param tableName
     *            the table name
     * @param indexName
     *            the index name
     * @param fieldName
     *            the field name
     */
    private void createIndex(String tableName, String indexName, String... fieldNames)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE INDEX IF NOT EXISTS ");
        builder.append(indexName);
        builder.append(" ON ");
        builder.append(tableName);
        builder.append(Constants.OPEN_ROUND_BRACKET);
        for (String fieldName : fieldNames)
        {
            builder.append(fieldName);
            builder.append(Constants.COMMA);
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(Constants.CLOSE_ROUND_BRACKET);
        StatementResult result = tableAPI.executeSync(builder.toString());
        if (!result.isSuccessful())
        {
            throw new SchemaGenerationException("Unable to CREATE Index with Index Name [" + indexName
                    + "] for table [" + tableName + "]");
        }
    }

    /**
     * Builds the create ddl query.
     * 
     * @param tableInfo
     *            the table info
     * @return the string
     */
    private String buildCreateDDLQuery(TableInfo tableInfo)
    {
        String statement;
        boolean flag = false;
        StringBuilder compoundKeys = null;
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ");
        builder.append(tableInfo.getTableName());
        builder.append(Constants.OPEN_ROUND_BRACKET);

        if (!tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
        {
            builder.append(tableInfo.getIdColumnName());
            builder.append(Constants.SPACE);
            String idType = tableInfo.getTableIdType().getSimpleName().toLowerCase();
            builder.append(OracleNoSQLValidationClassMapper.getValidIdType(idType));
            builder.append(Constants.COMMA);
        }

        for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
        {
            builder.append(columnInfo.getColumnName());
            builder.append(Constants.SPACE);
            String coulmnType = columnInfo.getType().getSimpleName().toLowerCase();
            builder.append(OracleNoSQLValidationClassMapper.getValidType(coulmnType));
            builder.append(Constants.COMMA);
        }
        for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
        {
            if (tableInfo.getIdColumnName().equals(embeddedColumnInfo.getEmbeddedColumnName()))
            {
                compoundKeys = new StringBuilder();
                flag = true;
            }
            for (ColumnInfo columnInfo : embeddedColumnInfo.getColumns())
            {
                builder.append(columnInfo.getColumnName());
                builder.append(Constants.SPACE);
                String coulmnType = columnInfo.getType().getSimpleName().toLowerCase();
                builder.append(OracleNoSQLValidationClassMapper.getValidType(coulmnType));
                builder.append(Constants.COMMA);
                if (flag)
                {
                    compoundKeys.append(columnInfo.getColumnName());
                    compoundKeys.append(Constants.COMMA);
                }
            }
            flag = false;
        }

        builder.append("PRIMARY KEY");
        builder.append(Constants.OPEN_ROUND_BRACKET);

        if (!tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
        {
            builder.append(tableInfo.getIdColumnName());
        }
        else
        {
            compoundKeys.deleteCharAt(compoundKeys.length() - 1);
            builder.append(compoundKeys.toString());
        }

        builder.append(Constants.CLOSE_ROUND_BRACKET);
        builder.append(Constants.CLOSE_ROUND_BRACKET);

        statement = builder.toString();
        return statement;
    }

    /**
     * Builds the alter ddl query.
     * 
     * @param tableInfo
     *            the table info
     * @param newColumns
     *            the new columns
     * @return the string
     */
    private String buildAlterDDLQuery(TableInfo tableInfo, Map<String, String> newColumns)
    {
        String statement;
        StringBuilder builder = new StringBuilder();
        builder.append("ALTER TABLE ");
        builder.append(tableInfo.getTableName());
        builder.append(Constants.OPEN_ROUND_BRACKET);

        for (Map.Entry<String, String> entry : newColumns.entrySet())
        {
            builder.append("ADD ");
            builder.append(entry.getKey());
            builder.append(Constants.SPACE);
            String coulmnType = entry.getValue().toLowerCase();
            builder.append(OracleNoSQLValidationClassMapper.getValidType(coulmnType));
            builder.append(Constants.COMMA);
        }

        builder.deleteCharAt(builder.length() - 1);
        builder.append(Constants.CLOSE_ROUND_BRACKET);
        statement = builder.toString();
        return statement;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * create_drop (java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos)
    {
        create(tableInfos);
    }

    /**
     * @param secProps
     * @param connectionProperties
     */
    private void setSecurityProps()
    {
        if (externalProperties.containsKey(KVSecurityConstants.TRANSPORT_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                    externalProperties.get(KVSecurityConstants.TRANSPORT_PROPERTY));
        }

        if (externalProperties.containsKey(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.SSL_PROTOCOLS_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SSL_PROTOCOLS_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SSL_PROTOCOLS_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.SSL_HOSTNAME_VERIFIER_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SSL_HOSTNAME_VERIFIER_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SSL_HOSTNAME_VERIFIER_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.SSL_CIPHER_SUITES_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SSL_CIPHER_SUITES_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SSL_CIPHER_SUITES_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.SECURITY_FILE_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                    externalProperties.get(KVSecurityConstants.SECURITY_FILE_PROPERTY));
        }

        if (externalProperties.containsKey(KVSecurityConstants.AUTH_WALLET_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.AUTH_WALLET_PROPERTY,
                    externalProperties.get(KVSecurityConstants.AUTH_WALLET_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.AUTH_USERNAME_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                    externalProperties.get(KVSecurityConstants.AUTH_USERNAME_PROPERTY));
        }
        if (externalProperties.containsKey(KVSecurityConstants.AUTH_PWDFILE_PROPERTY))
        {
            securityProps.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                    externalProperties.get(KVSecurityConstants.AUTH_PWDFILE_PROPERTY));
        }

    }

}
