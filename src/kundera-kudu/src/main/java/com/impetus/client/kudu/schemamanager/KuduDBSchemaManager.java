/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.schemamanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.client.AlterTableOptions;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.kudu.KuduDBValidationClassMapper;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class KuduDBSchemaManager.
 * 
 * @author karthikp.manchala
 */
public class KuduDBSchemaManager extends AbstractSchemaManager implements SchemaManager {

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(KuduDBSchemaManager.class);

    /** The client. */
    private KuduClient client;

    /**
     * Instantiates a new kudu db schema manager.
     * 
     * @param clientFactory
     *            the client factory
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public KuduDBSchemaManager(String clientFactory, Map<String, Object> externalProperties,
        KunderaMetadata kunderaMetadata) {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#exportSchema(java.lang.String,
     * java.util.List)
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas) {
        super.exportSchema(persistenceUnit, schemas);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema() {
        for (TableInfo tableinfo : tableInfos) {
            try {
                client.deleteTable(tableinfo.getTableName());
            } catch (Exception ex) {
                logger.error("Error during deleting tables in kudu, Caused by: ", ex);
                throw new SchemaGenerationException(ex, "Kudu");
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#validateEntity(java.lang.Class)
     */
    @Override
    public boolean validateEntity(Class clazz) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#initiateClient()
     */
    @Override
    protected boolean initiateClient() {
        for (String host : hosts) {
            if (host == null || !StringUtils.isNumeric(port) || port.isEmpty()) {
                logger.error("Host or port should not be null / port should be numeric");
                throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
            }
            try {
                client = new KuduClient.KuduClientBuilder(host + ":" + port).build();
            } catch (Exception e) {
                logger.error("Database host cannot be resolved, Caused by: " + e.getMessage());
                throw new SchemaGenerationException("Database host cannot be resolved, Caused by: " + e.getMessage());
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#validate(java.util.List)
     */
    @Override
    protected void validate(List<TableInfo> tableInfos) {
        for (TableInfo tableInfo : tableInfos) {
            try {
                if (!client.tableExists(tableInfo.getTableName())) {
                    throw new SchemaGenerationException("Table: " + tableInfo.getTableName() + " does not exist ");
                }
            } catch (Exception e) {
                logger.error("Error while validating tables, Caused by: " + e.getMessage());
                throw new KunderaException("Error while validating tables, Caused by: " + e.getMessage());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#update(java.util.List)
     */
    @Override
    protected void update(List<TableInfo> tableInfos) {
        for (TableInfo tableInfo : tableInfos) {
            try {
                if (!client.tableExists(tableInfo.getTableName())) {
                    createKuduTable(tableInfo);
                } else {
                    List<String> entityColumns = new ArrayList<String>();
                    KuduTable table = client.openTable(tableInfo.getTableName());
                    AlterTableOptions alterTableOptions = new AlterTableOptions();
                    AtomicBoolean updated = new AtomicBoolean(false);
                    Schema schema = table.getSchema();
                    // add modify columns
                    for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas()) {
                        entityColumns.add(columnInfo.getColumnName());
                        alterColumn(alterTableOptions, schema, columnInfo, updated);
                    }
                    // delete columns
                    for (ColumnSchema columnSchema : schema.getColumns()) {
                        // if not in tableInfo and not a key then delete
                        if (!entityColumns.contains(columnSchema.getName()) && !columnSchema.isKey()) {
                            alterTableOptions.dropColumn(columnSchema.getName());
                            updated.set(true);
                        }
                    }

                    if (updated.get()) {
                        client.alterTable(tableInfo.getTableName(), alterTableOptions);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while updating tables, Caused by: " + e.getMessage());
                throw new KunderaException("Error while updating tables, Caused by: " + e.getMessage());
            }
        }
    }

    /**
     * Alter column.
     * 
     * @param alterTableOptions
     *            the alter table options
     * @param schema
     *            the schema
     * @param columnInfo
     *            the column info
     * @param updated
     *            the updated
     */
    private void alterColumn(AlterTableOptions alterTableOptions, Schema schema, ColumnInfo columnInfo,
        AtomicBoolean updated) {
        if (!hasColumn(schema, columnInfo.getColumnName())) {
            // add if column is not in schema
            alterTableOptions.addNullableColumn(columnInfo.getColumnName(),
                KuduDBValidationClassMapper.getValidTypeForClass(columnInfo.getType()));
            updated.set(true);
        } else {
            // check for type, drop and add if not consistent TODO: throw exception or override?
            if (!schema.getColumn(columnInfo.getColumnName()).getType()
                .equals(KuduDBValidationClassMapper.getValidTypeForClass(columnInfo.getType()))) {
                alterTableOptions.dropColumn(columnInfo.getColumnName());
                alterTableOptions.addNullableColumn(columnInfo.getColumnName(),
                    KuduDBValidationClassMapper.getValidTypeForClass(columnInfo.getType()));
                updated.set(true);
            }
        }
    }

    /**
     * Checks for column.
     * 
     * @param schema
     *            the schema
     * @param columnName
     *            the column name
     * @return true, if successful
     */
    private boolean hasColumn(Schema schema, String columnName) {
        try {
            schema.getColumn(columnName);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create(java.util.List)
     */
    @Override
    protected void create(List<TableInfo> tableInfos) {

        for (TableInfo tableInfo : tableInfos) {
            createKuduTable(tableInfo);
        }
    }

    /**
     * Creates the kudu table.
     * 
     * @param tableInfo
     *            the table info
     */
    private void createKuduTable(TableInfo tableInfo) {
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        // add key
        columns.add(new ColumnSchema.ColumnSchemaBuilder(tableInfo.getIdColumnName(), KuduDBValidationClassMapper
            .getValidTypeForClass(tableInfo.getTableIdType())).key(true).build());
        // add other columns
        for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas()) {
            ColumnSchemaBuilder columnSchemaBuilder =
                new ColumnSchema.ColumnSchemaBuilder(columnInfo.getColumnName(),
                    KuduDBValidationClassMapper.getValidTypeForClass(columnInfo.getType()));
            columns.add(columnSchemaBuilder.build());
        }
        Schema schema = new Schema(columns);
        try {
            client.createTable(tableInfo.getTableName(), schema);
            logger.debug("Table: " + tableInfo.getTableName() + " created successfully");
        } catch (Exception e) {
            logger.error("Table: " + tableInfo.getTableName() + " cannot be created, Caused by: " + e.getMessage(), e);
            throw new SchemaGenerationException("Table: " + tableInfo.getTableName() + " cannot be created, Caused by"
                + e.getMessage(), e, "Kudu");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create_drop(java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos) {
        create(tableInfos);
    }

}
