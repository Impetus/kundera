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
package com.impetus.client.cassandra.schemamanager;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.Constants;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.DataCenter;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.Table;
import com.impetus.kundera.configure.schema.CollectionColumnInfo;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.IndexInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Manages auto schema operation defined in {@code ScheamOperationType}.
 * 
 * @author Kuldeep.kumar
 * 
 */
public class CassandraSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    private static final String STANDARDCOLUMNFAMILY = "Standard";

    /**
     * Cassandra client variable holds the client.
     */
    private Cassandra.Client cassandra_client;

    private String cql_version = CassandraConstants.CQL_VERSION_2_0;

    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaManager.class);

    /** The csmd. */
    private CassandraSchemaMetadata csmd = CassandraPropertyReader.csmd;

    /** The tables. */
    private List<Table> tables;

    /**
     * Instantiates a new cassandra schema manager.
     * 
     * @param clientFactory
     *            the configured client clientFactory
     * @param puProperties
     */
    public CassandraSchemaManager(String clientFactory, Map<String, Object> puProperties)
    {
        super(clientFactory, puProperties);
    }

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        cql_version = externalProperties != null ? (String) externalProperties.get(CassandraConstants.CQL_VERSION)
                : CassandraConstants.CQL_VERSION_2_0;
        super.exportSchema(persistenceUnit, schemas);
    }

    /**
     * drop schema method drop the table from keyspace.
     * 
     */
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            try
            {
                cassandra_client.set_keyspace(databaseName);
                for (TableInfo tableInfo : tableInfos)
                {
                    dropColumnFamily(tableInfo);
                }
            }
            catch (Exception ex)
            {
                log.error("Error during dropping schema in cassandra, Caused by: .", ex);
                throw new SchemaGenerationException(ex, "Cassandra");
            }
        }
        cassandra_client = null;
    }

    /**
     * Drops column family specified in table info.
     * 
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void dropColumnFamily(TableInfo tableInfo) throws Exception
    {
        if (containsCompositeKey(tableInfo))
        {
            dropTableUsingCql(tableInfo);
        }
        else
        {
            cassandra_client.system_drop_column_family(tableInfo.getTableName());
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
     * Creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create(List<TableInfo> tableInfos)
    {
        try
        {
            createOrUpdate(tableInfos);
        }
        catch (Exception ex)
        {
            throw new PropertyAccessException(ex);
        }
    }

    /**
     * Creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     * @throws TimedOutException
     * @throws UnavailableException
     */
    private void createOrUpdate(List<TableInfo> tableInfos) throws Exception
    {
        KsDef ksDef = onCreateKeyspace(); // create keyspace event
        createColumnFamilies(tableInfos, ksDef); // create column family
                                                 // event.
    }

    private KsDef onCreateKeyspace() throws Exception
    {
        try
        {
            createKeyspace();
        }
        catch (InvalidRequestException irex)
        {
            // Ignore and add a log.debug
        }
        // keyspace already exists.
        cassandra_client.set_keyspace(databaseName);
        return cassandra_client.describe_keyspace(databaseName);
    }

    /**
     * Creates keyspace.
     * 
     * @return
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private KsDef createKeyspace() throws Exception
    {
        Map<String, String> strategy_options = new HashMap<String, String>();
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        KsDef ksDef = new KsDef(databaseName, csmd.getPlacement_strategy(databaseName), cfDefs);
        setProperties(ksDef, strategy_options);
        ksDef.setStrategy_options(strategy_options);
        cassandra_client.system_add_keyspace(ksDef);
        return ksDef;
    }

    /**
     * 
     * @param tableInfos
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     * @throws UnsupportedEncodingException
     * @throws NotFoundException
     * @throws UnavailableException
     * @throws TimedOutException
     */
    private void createColumnFamilies(List<TableInfo> tableInfos, KsDef ksDef) throws Exception
    {
        for (TableInfo tableInfo : tableInfos)
        {
            createOrUpdateColumnFamily(tableInfo, ksDef);

            // Create Index Table if required
            createInvertedIndexTable(tableInfo);

        }
    }

    /**
     * 
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     * @throws NotFoundException
     * @throws UnsupportedEncodingException
     * @throws UnavailableException
     * @throws TimedOutException
     */
    private void createOrUpdateColumnFamily(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        MetaDataHandler handler = new MetaDataHandler();
        if (containsCompositeKey(tableInfo))
        {
            validateCompoundKey(tableInfo);
            onCompoundKey(tableInfo, ksDef);

            // After successful schema operation, perform index creation.
            createIndexUsingCql(tableInfo);
        }
        else if (containsCollectionColumns(tableInfo))
        {
            createOrUpdateUsingCQL3(tableInfo, ksDef);
            createIndexUsingCql(tableInfo);
        }
        else
        {
            CfDef cf_def = handler.getTableMetadata(tableInfo);
            try
            {
                cassandra_client.system_add_column_family(cf_def);
            }
            catch (InvalidRequestException irex)
            {
                updateExistingColumnFamily(tableInfo, ksDef, irex);
            }
        }
    }

    /**
     * Creates (or updates) a column family definition using CQL 3 Should
     * replace onCompoundKey
     * 
     * @param tableInfo
     * @param ksDef
     * @throws Exception
     */
    private void createOrUpdateUsingCQL3(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        CQLTranslator translator = new CQLTranslator();
        String columnFamilyQuery = CQLTranslator.CREATE_COLUMNFAMILY_QUERY;
        columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableInfo.getTableName()).toString());

        List<ColumnInfo> columns = tableInfo.getColumnMetadatas();

        StringBuilder queryBuilder = new StringBuilder();

        // For normal columns
        onCompositeColumns(translator, columns, queryBuilder, null);
        onCollectionColumns(translator, tableInfo.getCollectionColumnMetadatas(), queryBuilder);

        // ideally it will always be one as more super column families
        // are not allowed with compound/composite key.
        List<EmbeddedColumnInfo> compositeColumns = tableInfo.getEmbeddedColumnMetadatas();
        EmbeddableType compoEmbeddableType = null;
        if (!compositeColumns.isEmpty())
        {
            compoEmbeddableType = compositeColumns.get(0).getEmbeddable();
            onCompositeColumns(translator, compositeColumns.get(0).getColumns(), queryBuilder, columns);
        }
        else
        {
            String dataType = CassandraValidationClassMapper.getValidationClass(tableInfo.getTableIdType(), true);
            String cqlType = translator.getCQLType(dataType);
            translator.appendColumnName(queryBuilder, tableInfo.getIdColumnName(), cqlType);
            queryBuilder.append(" ,");
        }

        queryBuilder = stripLastChar(columnFamilyQuery, queryBuilder);

        // append primary key clause
        queryBuilder.append(translator.ADD_PRIMARYKEY_CLAUSE);

        Field[] fields = tableInfo.getTableIdType().getDeclaredFields();

        // To ensure field ordering
        if (compoEmbeddableType != null)
        {
            StringBuilder primaryKeyBuilder = new StringBuilder();
            appendPrimaryKey(translator, compoEmbeddableType, fields, primaryKeyBuilder);
            // should not be null.
            primaryKeyBuilder.deleteCharAt(primaryKeyBuilder.length() - 1);
            queryBuilder = new StringBuilder(StringUtils.replace(queryBuilder.toString(), CQLTranslator.COLUMNS,
                    primaryKeyBuilder.toString()));
        }
        else
        {
            queryBuilder = new StringBuilder(StringUtils.replace(queryBuilder.toString(), CQLTranslator.COLUMNS,
                    tableInfo.getIdColumnName()));
        }

        // set column family properties defined in configuration property/xml
        // files.
        setColumnFamilyProperties(null, getColumnFamilyProperties(tableInfo), queryBuilder);

        try
        {
            cassandra_client.set_cql_version(CassandraConstants.CQL_VERSION_3_0);
            cassandra_client.set_keyspace(databaseName);
            cassandra_client.execute_cql3_query(
                    ByteBuffer.wrap(queryBuilder.toString().getBytes(Constants.CHARSET_UTF8)), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException irex)
        {
            updateExistingColumnFamily(tableInfo, ksDef, irex);
        }

    }

    private boolean containsCollectionColumns(TableInfo tableInfo)
    {
        return !tableInfo.getCollectionColumnMetadatas().isEmpty();
    }

    private boolean containsCompositeKey(TableInfo tableInfo)
    {
        return tableInfo.getTableIdType() != null && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class);
    }

    private void updateExistingColumnFamily(TableInfo tableInfo, KsDef ksDef, InvalidRequestException irex)
            throws Exception
    {
        StringBuilder builder = new StringBuilder("Cannot add already existing column family ");

        if (irex.getWhy() != null && irex.getWhy().contains(builder.toString()))
        {
            SchemaOperationType operationType = SchemaOperationType.getInstance(operation);
            switch (operationType)
            {
            case create:
                handleCreate(tableInfo, ksDef);

            case createdrop:
                handleCreate(tableInfo, ksDef);
                break;

            case update:
                if (isCql3Enabled(tableInfo))
                {
                    for (ColumnInfo column : tableInfo.getColumnMetadatas())
                    {
                        addColumnToTable(tableInfo, column);
                    }
                }
                updateTable(ksDef, tableInfo);
                break;

            default:
                break;
            }
        }
        else
        {
            log.error("Error occurred while creating table{}, Caused by: .", tableInfo.getTableName(), irex);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(),
                    irex, "Cassandra", databaseName);
        }
    }

    private void handleCreate(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        if (containsCompositeKey(tableInfo))
        {
            validateCompoundKey(tableInfo);
            // First drop existing column family.
            dropTableUsingCql(tableInfo);
        }
        else
        {
            onDrop(tableInfo);
        }
        createOrUpdateColumnFamily(tableInfo, ksDef);
    }

    private void onDrop(TableInfo tableInfo) throws Exception
    {
        dropColumnFamily(tableInfo);
        dropInvertedIndexTable(tableInfo);
    }

    /**
     * update method update schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void update(List<TableInfo> tableInfos)
    {
        try
        {
            createOrUpdate(tableInfos);
        }
        catch (Exception ex)
        {
            log.error("Error occurred while creating {}, Caused by: .", databaseName, ex);
            throw new SchemaGenerationException(ex);
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
        try
        {
            KsDef ksDef = cassandra_client.describe_keyspace(databaseName);
            onValidateTables(tableInfos, ksDef);
        }
        catch (Exception ex)
        {
            log.error("Error occurred while validating {}, Caused by: .", databaseName, ex);
            throw new SchemaGenerationException(ex);
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
        Throwable message = null;
        for (String host : hosts)
        {
            if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
            {
                log.error("Host or port should not be null, Port should be numeric.");
                throw new IllegalArgumentException("Host or port should not be null, Port should be numeric.");
            }
            TSocket socket = new TSocket(host, Integer.parseInt(port));
            TTransport transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            cassandra_client = new Cassandra.Client(protocol);
            try
            {
                if (!socket.isOpen())
                {
                    socket.open();
                    if (userName != null)
                    {
                        Map<String, String> credentials = new HashMap<String, String>();
                        credentials.put("username", userName);
                        credentials.put("password", password);
                        AuthenticationRequest auth_request = new AuthenticationRequest(credentials);
                        cassandra_client.login(auth_request);
                    }
                }
                return true;
            }
            catch (TTransportException e)
            {
                message = e;
                log.warn("Error while opening socket for host {}, skipping for next available node ", host);
            }
            catch (Exception e)
            {
                log.error("Error during creating schema in cassandra, Caused by: .", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
        }
        throw new SchemaGenerationException("Error while opening socket, Caused by: .", message, "Cassandra");
    }

    /**
     * On compound key.
     * 
     * @param tableInfo
     *            the table infos
     * @throws TimedOutException
     * @throws UnavailableException
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws TException
     *             the t exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void onCompoundKey(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        CQLTranslator translator = new CQLTranslator();
        String columnFamilyQuery = CQLTranslator.CREATE_COLUMNFAMILY_QUERY;
        columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableInfo.getTableName()).toString());

        List<ColumnInfo> columns = tableInfo.getColumnMetadatas();

        StringBuilder queryBuilder = new StringBuilder();

        // for normal columns
        onCompositeColumns(translator, columns, queryBuilder, null);

        // ideally it will always be one as more super column families
        // are not allowed with compound/composite key.

        List<EmbeddedColumnInfo> compositeColumns = tableInfo.getEmbeddedColumnMetadatas();
        EmbeddableType compoEmbeddableType = compositeColumns.get(0).getEmbeddable();

        // for composite columns
        onCompositeColumns(translator, compositeColumns.get(0).getColumns(), queryBuilder, columns);

        queryBuilder = stripLastChar(columnFamilyQuery, queryBuilder);

        // append primary key clause

        queryBuilder.append(translator.ADD_PRIMARYKEY_CLAUSE);

        Field[] fields = tableInfo.getTableIdType().getDeclaredFields();

        StringBuilder primaryKeyBuilder = new StringBuilder();

        // To ensure field ordering
        appendPrimaryKey(translator, compoEmbeddableType, fields, primaryKeyBuilder);

        // should not be null.
        primaryKeyBuilder.deleteCharAt(primaryKeyBuilder.length() - 1);

        queryBuilder = new StringBuilder(StringUtils.replace(queryBuilder.toString(), CQLTranslator.COLUMNS,
                primaryKeyBuilder.toString()));

        // set column family properties defined in configuration property/xml
        // files.
        setColumnFamilyProperties(null, getColumnFamilyProperties(tableInfo), queryBuilder);

        try
        {
            cassandra_client.set_cql_version(CassandraConstants.CQL_VERSION_3_0);
            cassandra_client.set_keyspace(databaseName);
            cassandra_client.execute_cql3_query(
                    ByteBuffer.wrap(queryBuilder.toString().getBytes(Constants.CHARSET_UTF8)), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException irex)
        {
            updateExistingColumnFamily(tableInfo, ksDef, irex);
        }

    }

    private void appendPrimaryKey(CQLTranslator translator, EmbeddableType compoEmbeddableType, Field[] fields,
            StringBuilder primaryKeyBuilder)
    {
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                Attribute attribute = compoEmbeddableType.getAttribute(f.getName());
                translator.appendColumnName(primaryKeyBuilder, ((AbstractAttribute) attribute).getJPAColumnName());
                primaryKeyBuilder.append(" ,");
            }
        }
    }

    private StringBuilder stripLastChar(String columnFamilyQuery, StringBuilder queryBuilder)
    {
        // strip last ",".
        if (queryBuilder.length() > 0)
        {
            queryBuilder.deleteCharAt(queryBuilder.length() - 1);

            columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMNS, queryBuilder.toString());
            queryBuilder = new StringBuilder(columnFamilyQuery);
        }
        return queryBuilder;
    }

    private void createIndexUsingThrift(TableInfo tableInfo, CfDef cfDef) throws Exception
    {
        for (IndexInfo indexInfo : tableInfo.getColumnsToBeIndexed())
        {
            for (ColumnDef columnDef : cfDef.getColumn_metadata())
            {
                if (new String(columnDef.getName(), Constants.ENCODING).equals(indexInfo.getColumnName()))
                {
                    columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
                }
            }
        }
        cassandra_client.system_update_column_family(cfDef);
    }

    /**
     * Create secondary indexes on columns.
     * 
     * @param tableInfo
     */
    private void createIndexUsingCql(TableInfo tableInfo) throws Exception
    {
        StringBuilder indexQueryBuilder = new StringBuilder("create index $COLUMN_NAME on \"");
        indexQueryBuilder.append(tableInfo.getTableName());
        indexQueryBuilder.append("\"(\"$COLUMN_NAME\")");
        for (IndexInfo indexInfo : tableInfo.getColumnsToBeIndexed())
        {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(indexInfo.getColumnName());
            if (!tableInfo.getEmbeddedColumnMetadatas().isEmpty())
            {
                List<ColumnInfo> columnInfos = tableInfo.getEmbeddedColumnMetadatas().get(0).getColumns();
                if (columnInfos.contains(columnInfo))
                {
                    return;
                }
            }
            String replacedWithindexName = StringUtils.replace(indexQueryBuilder.toString(), "$COLUMN_NAME",
                    indexInfo.getColumnName());
            try
            {
                cassandra_client.execute_cql3_query(ByteBuffer.wrap(replacedWithindexName.getBytes()),
                        Compression.NONE, ConsistencyLevel.ONE);
            }
            catch (InvalidRequestException ire)
            {
                if (ire.getWhy() != null && !ire.getWhy().equals("Index already exists")
                        && operation.equalsIgnoreCase(SchemaOperationType.update.name()))
                {
                    log.error("Error occurred while creating indexes on column{} of table {}, , Caused by: .",
                            indexInfo.getColumnName(), tableInfo.getTableName(), ire);
                    throw new SchemaGenerationException("Error occurred while creating indexes on column "
                            + indexInfo.getColumnName() + " of table " + tableInfo.getTableName(), ire, "Cassandra",
                            databaseName);
                }
            }
        }
    }

    /**
     * Drops table using cql3.
     * 
     * @param tableInfo
     */
    private void dropTableUsingCql(TableInfo tableInfo) throws Exception
    {
        CQLTranslator translator = new CQLTranslator();
        StringBuilder dropQuery = new StringBuilder("drop table ");
        translator.ensureCase(dropQuery, tableInfo.getTableName());
        cassandra_client.execute_cql3_query(ByteBuffer.wrap(dropQuery.toString().getBytes()), Compression.NONE,
                ConsistencyLevel.ONE);
    }

    /**
     * Adds column to table if not exists previously i.e. alter table.
     * 
     * @param tableInfo
     * @param translator
     * @param column
     * @throws TException
     * @throws SchemaDisagreementException
     * @throws TimedOutException
     * @throws UnavailableException
     * @throws InvalidRequestException
     */
    private void addColumnToTable(TableInfo tableInfo, ColumnInfo column) throws Exception
    {
        CQLTranslator translator = new CQLTranslator();
        StringBuilder addColumnQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(addColumnQuery, tableInfo.getTableName());
        addColumnQuery.append(" ADD ");
        translator.ensureCase(addColumnQuery, column.getColumnName());
        addColumnQuery.append(" "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType(),
                        isCql3Enabled(tableInfo))));
        try
        {
            cassandra_client.execute_cql3_query(ByteBuffer.wrap(addColumnQuery.toString().getBytes()),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException ireforAddColumn)
        {
            StringBuilder ireforAddColumnbBuilder = new StringBuilder("Invalid column name ");
            ireforAddColumnbBuilder.append(column.getColumnName() + " because it conflicts with an existing column");
            if (ireforAddColumn.getWhy() != null && ireforAddColumn.getWhy().equals(ireforAddColumnbBuilder.toString()))
            {
                alterColumnType(tableInfo, translator, column);
            }
            else
            {
                log.error("Error occurred while altering column type of  table {}, Caused by: .",
                        tableInfo.getTableName(), ireforAddColumn);
                throw new SchemaGenerationException("Error occurred while adding column into table "
                        + tableInfo.getTableName(), ireforAddColumn, "Cassandra", databaseName);
            }
        }

    }

    /**
     * Alters column type of an existing column.
     * 
     * @param tableInfo
     * @param translator
     * @param column
     * @throws TException
     * @throws SchemaDisagreementException
     * @throws TimedOutException
     * @throws UnavailableException
     * @throws InvalidRequestException
     */
    private void alterColumnType(TableInfo tableInfo, CQLTranslator translator, ColumnInfo column) throws Exception
    {
        StringBuilder alterColumnTypeQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(alterColumnTypeQuery, tableInfo.getTableName());
        alterColumnTypeQuery.append(" ALTER ");
        translator.ensureCase(alterColumnTypeQuery, column.getColumnName());
        alterColumnTypeQuery.append(" TYPE "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType(),
                        isCql3Enabled(tableInfo))));
        cassandra_client.execute_cql3_query(ByteBuffer.wrap(alterColumnTypeQuery.toString().getBytes()),
                Compression.NONE, ConsistencyLevel.ONE);
    }

    /**
     * On composite columns.
     * 
     * @param translator
     *            the translator
     * @param columns
     *            the columns
     * @param queryBuilder
     *            the query builder
     */
    private void onCompositeColumns(CQLTranslator translator, List<ColumnInfo> compositeColumns,
            StringBuilder queryBuilder, List<ColumnInfo> columns)
    {
        for (ColumnInfo colInfo : compositeColumns)
        {
            if (columns == null || (columns != null && !columns.contains(colInfo)))
            {
                String dataType = CassandraValidationClassMapper.getValidationClass(colInfo.getType(), true);
                String cqlType = translator.getCQLType(dataType);
                translator.appendColumnName(queryBuilder, colInfo.getColumnName(), cqlType);
                queryBuilder.append(" ,");
            }
        }
    }

    /**
     * Generates schema for Collection columns
     * 
     * @param translator
     * @param collectionColumnInfos
     * @param queryBuilder
     */
    private void onCollectionColumns(CQLTranslator translator, List<CollectionColumnInfo> collectionColumnInfos,
            StringBuilder queryBuilder)
    {
        for (CollectionColumnInfo cci : collectionColumnInfos)
        {
            String dataType = CassandraValidationClassMapper.getValidationClass(cci.getType(), true);

            // CQL Type of collection column
            String collectionCqlType = translator.getCQLType(dataType);

            // Collection Column Name
            String collectionColumnName = new String(cci.getCollectionColumnName());

            // Generic Type list
            StringBuilder genericTypesBuilder = null;
            List<Class<?>> genericClasses = cci.getGenericClasses();
            if (!genericClasses.isEmpty())
            {
                genericTypesBuilder = new StringBuilder();
                if (MapType.class.getSimpleName().equals(dataType) && genericClasses.size() == 2)
                {
                    genericTypesBuilder.append("<");
                    String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0), true);
                    genericTypesBuilder.append(translator.getCQLType(keyDataType));
                    genericTypesBuilder.append(",");
                    String valueDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(1),
                            true);
                    genericTypesBuilder.append(translator.getCQLType(valueDataType));
                    genericTypesBuilder.append(">");
                }
                else if ((ListType.class.getSimpleName().equals(dataType) || SetType.class.getSimpleName().equals(
                        dataType))
                        && genericClasses.size() == 1)
                {
                    genericTypesBuilder.append("<");
                    String valueDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0),
                            true);
                    genericTypesBuilder.append(translator.getCQLType(valueDataType));
                    genericTypesBuilder.append(">");
                }
                else
                {
                    throw new SchemaGenerationException("Incorrect collection field definition for "
                            + cci.getCollectionColumnName() + ". Genric Types must be defined correctly.");
                }
            }

            if (genericTypesBuilder != null)
            {
                collectionCqlType += genericTypesBuilder.toString();
            }

            translator.appendColumnName(queryBuilder, collectionColumnName, collectionCqlType);
            queryBuilder.append(" ,");

        }
    }

    /**
     * Creates the inverted index table.
     * 
     * @param tableInfo
     *            the table info
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
     */
    private void createInvertedIndexTable(TableInfo tableInfo) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        CfDef cfDef = getInvertedIndexCF(tableInfo);
        if (cfDef != null)
        {
            cassandra_client.system_add_column_family(cfDef);
        }
    }

    /**
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private CfDef getInvertedIndexCF(TableInfo tableInfo) throws InvalidRequestException, SchemaDisagreementException,
            TException
    {
        boolean indexTableRequired = CassandraPropertyReader.csmd.isInvertedIndexingEnabled(databaseName)
                && !tableInfo.getEmbeddedColumnMetadatas().isEmpty();
        if (indexTableRequired)
        {
            CfDef cfDef = new CfDef();
            cfDef.setKeyspace(databaseName);
            cfDef.setColumn_type("Super");
            cfDef.setName(tableInfo.getTableName() + Constants.INDEX_TABLE_SUFFIX);
            cfDef.setKey_validation_class(UTF8Type.class.getSimpleName());
            return cfDef;
        }
        return null;
    }

    /**
     * Drop inverted index table.
     * 
     * @param tableInfo
     *            the table info
     */
    private void dropInvertedIndexTable(TableInfo tableInfo)
    {
        boolean indexTableRequired = CassandraPropertyReader.csmd.isInvertedIndexingEnabled(databaseName)/* ) */
                && !tableInfo.getEmbeddedColumnMetadatas().isEmpty();
        if (indexTableRequired)
        {
            try
            {
                cassandra_client.system_drop_column_family(tableInfo.getTableName() + Constants.INDEX_TABLE_SUFFIX);
            }
            catch (Exception ex)
            {
                if (log.isWarnEnabled())
                {
                    log.warn("Error while dropping inverted index table, Caused by: ", ex);
                }
            }
        }
    }

    /**
     * check for Tables method check the existence of schema and table.
     * 
     * @param tableInfos
     *            list of TableInfos and ksDef object of KsDef
     * @param ksDef
     *            the ks def
     * @throws TException
     * @throws InvalidRequestException
     */
    private void onValidateTables(List<TableInfo> tableInfos, KsDef ksDef) throws Exception
    {
        cassandra_client.set_keyspace(ksDef.getName());
        for (TableInfo tableInfo : tableInfos)
        {
            onValidateTable(ksDef, tableInfo);
        }
    }

    private void onValidateTable(KsDef ksDef, TableInfo tableInfo) throws Exception
    {
        boolean tablefound = false;
        for (CfDef cfDef : ksDef.getCf_defs())
        {
            if (cfDef.getName().equals(tableInfo.getTableName())
                    && (cfDef.getColumn_type().equals(ColumnFamilyType.getInstanceOf(tableInfo.getType()).name())))
            {
                if (cfDef.getColumn_type().equals(ColumnFamilyType.Standard.name()))
                {
                    for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                    {
                        onValidateColumn(tableInfo, cfDef, columnInfo);
                    }
                    tablefound = true;
                    break;
                }
                else if (cfDef.getColumn_type().equals(ColumnFamilyType.Super.name()))
                {
                    tablefound = true;
                }
            }
        }
        if (!tablefound)
        {
            throw new SchemaGenerationException("Column family " + tableInfo.getTableName()
                    + " does not exist in keyspace " + databaseName + "", "Cassandra", databaseName,
                    tableInfo.getTableName());
        }
    }

    private void onValidateColumn(TableInfo tableInfo, CfDef cfDef, ColumnInfo columnInfo) throws Exception
    {
        boolean columnfound = false;
        for (ColumnDef columnDef : cfDef.getColumn_metadata())
        {
            if (isMetadataSame(columnDef, columnInfo, isCql3Enabled(tableInfo)))
            {
                columnfound = true;
                break;
            }
        }
        if (!columnfound)
        {
            throw new SchemaGenerationException("Column " + columnInfo.getColumnName()
                    + " does not exist in column family " + tableInfo.getTableName() + "", "Cassandra", databaseName,
                    tableInfo.getTableName());
        }
    }

    /**
     * is metadata same method returns true if ColumnDef and columnInfo have
     * same metadata.
     * 
     * @param columnDef
     *            the column def
     * @param columnInfo
     *            the column info
     * @return true, if is metadata same
     * @throws UnsupportedEncodingException
     *             the unsupported encoding exception
     */
    private boolean isMetadataSame(ColumnDef columnDef, ColumnInfo columnInfo, boolean isCql3Enabled) throws Exception
    {
        return isIndexPresent(columnInfo, columnDef, isCql3Enabled);
    }

    /**
     * 
     * @param ksDef
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     * @throws UnsupportedEncodingException
     */
    private void updateTable(KsDef ksDef, TableInfo tableInfo) throws Exception
    {
        for (CfDef cfDef : ksDef.getCf_defs())
        {
            if (cfDef.getName().equals(tableInfo.getTableName())
                    && cfDef.getColumn_type().equals(ColumnFamilyType.getInstanceOf(tableInfo.getType()).name()))
            {
                boolean toUpdate = false;
                if (cfDef.getColumn_type().equals(STANDARDCOLUMNFAMILY))
                {
                    for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                    {
                        toUpdate = isCfDefUpdated(columnInfo, cfDef, isCql3Enabled(tableInfo), tableInfo) ? true
                                : toUpdate;
                    }
                }
                if (toUpdate)
                {
                    cassandra_client.system_update_column_family(cfDef);
                    createIndexUsingThrift(tableInfo, cfDef);
                }
                break;
            }
        }
    }

    private boolean isCfDefUpdated(ColumnInfo columnInfo, CfDef cfDef, boolean isCql3Enabled, TableInfo tableInfo)
            throws Exception
    {
        boolean columnPresent = false;
        boolean isUpdated = false;
        for (ColumnDef columnDef : cfDef.getColumn_metadata())
        {
            if (isColumnPresent(columnInfo, columnDef, isCql3Enabled))
            {
                if (!isValidationClassSame(columnInfo, columnDef, isCql3Enabled))
                {
                    columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(
                            columnInfo.getType(), isCql3Enabled));
                    columnDef.setIndex_typeIsSet(false);
                    columnDef.setIndex_nameIsSet(false);
                    isUpdated = true;
                }
                columnPresent = true;
                break;
            }
        }
        if (!columnPresent)
        {
            cfDef.addToColumn_metadata(getColumnMetadata(columnInfo, tableInfo));
            isUpdated = true;
        }
        return isUpdated;
    }

    /**
     * isInedexesPresent method return whether indexes present or not on
     * particular column.
     * 
     * @param columnInfo
     *            the column info
     * @param cfDef
     *            the cf def
     * @return true, if is indexes present
     * @throws UnsupportedEncodingException
     */
    private boolean isColumnPresent(ColumnInfo columnInfo, ColumnDef columnDef, boolean isCql3Enabled) throws Exception
    {
        return (new String(columnDef.getName(), Constants.ENCODING).equals(columnInfo.getColumnName()));
    }

    /**
     * isInedexesPresent method return whether indexes present or not on
     * particular column.
     * 
     * @param columnInfo
     *            the column info
     * @param cfDef
     *            the cf def
     * @return true, if is indexes present
     * @throws UnsupportedEncodingException
     */
    private boolean isValidationClassSame(ColumnInfo columnInfo, ColumnDef columnDef, boolean isCql3Enabled)
            throws Exception
    {
        return (isColumnPresent(columnInfo, columnDef, isCql3Enabled) && columnDef.getValidation_class().endsWith(
                CassandraValidationClassMapper.getValidationClass(columnInfo.getType(), isCql3Enabled)));
    }

    /**
     * isInedexesPresent method return whether indexes present or not on
     * particular column.
     * 
     * @param columnInfo
     *            the column info
     * @param cfDef
     *            the cf def
     * @return true, if is indexes present
     * @throws UnsupportedEncodingException
     */
    private boolean isIndexPresent(ColumnInfo columnInfo, ColumnDef columnDef, boolean isCql3Enabled) throws Exception
    {
        return (isValidationClassSame(columnInfo, columnDef, isCql3Enabled) && (columnDef.isSetIndex_type() == columnInfo
                .isIndexable() || (columnDef.isSetIndex_type())));
    }

    /**
     * getColumnMetadata use for getting column metadata for specific
     * columnInfo.
     * 
     * @param columnInfo
     *            the column info
     * @return the column metadata
     */
    private ColumnDef getColumnMetadata(ColumnInfo columnInfo, TableInfo tableInfo)
    {
        ColumnDef columnDef = new ColumnDef();
        columnDef.setName(columnInfo.getColumnName().getBytes());
        columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(columnInfo.getType(),
                isCql3Enabled(tableInfo)));

        if (columnInfo.isIndexable())
        {
            IndexInfo indexInfo = tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
            columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
        }
        return columnDef;
    }

    /**
     * Sets the properties.
     * 
     * @param ksDef
     *            the ks def
     * @param strategy_options
     *            the strategy_options
     */
    private void setProperties(KsDef ksDef, Map<String, String> strategy_options)
    {
        Schema schema = CassandraPropertyReader.csmd.getSchema(databaseName);
        if (schema != null && schema.getName() != null && schema.getName().equalsIgnoreCase(databaseName)
                && schema.getSchemaProperties() != null)
        {
            setKeyspaceProperties(ksDef, schema.getSchemaProperties(), strategy_options, schema.getDataCenters());
        }
        else
        {
            setDefaultReplicationFactor(strategy_options);
        }
    }

    /**
     * @param strategy_options
     */
    private void setDefaultReplicationFactor(Map<String, String> strategy_options)
    {
        strategy_options.put("replication_factor", CassandraConstants.DEFAULT_REPLICATION_FACTOR);
    }

    /**
     * Sets the keyspace properties.
     * 
     * @param ksDef
     *            the ks def
     * @param schemaProperties
     *            the schema properties
     * @param strategyOptions
     *            the strategy options
     * @param dcs
     *            the dcs
     */
    private void setKeyspaceProperties(KsDef ksDef, Properties schemaProperties, Map<String, String> strategyOptions,
            List<DataCenter> dcs)
    {
        String placementStrategy = schemaProperties.getProperty(CassandraConstants.PLACEMENT_STRATEGY,
                SimpleStrategy.class.getSimpleName());
        if (placementStrategy.equalsIgnoreCase(SimpleStrategy.class.getSimpleName())
                || placementStrategy.equalsIgnoreCase(SimpleStrategy.class.getName()))
        {
            String replicationFactor = schemaProperties.getProperty(CassandraConstants.REPLICATION_FACTOR,
                    CassandraConstants.DEFAULT_REPLICATION_FACTOR);
            strategyOptions.put("replication_factor", replicationFactor);
        }
        else if (placementStrategy.equalsIgnoreCase(NetworkTopologyStrategy.class.getSimpleName())
                || placementStrategy.equalsIgnoreCase(NetworkTopologyStrategy.class.getName()))
        {
            if (dcs != null && !dcs.isEmpty())
            {
                for (DataCenter dc : dcs)
                {
                    strategyOptions.put(dc.getName(), dc.getValue());
                }
            }
        }
        else
        {
            strategyOptions.put("replication_factor", CassandraConstants.DEFAULT_REPLICATION_FACTOR);
        }

        ksDef.setStrategy_class(placementStrategy);
        ksDef.setDurable_writes(Boolean.parseBoolean(schemaProperties.getProperty(CassandraConstants.DURABLE_WRITES)));
    }

    /**
     * Gets the column family properties.
     * 
     * @param tableInfo
     *            the table info
     * @return the column family properties
     */
    private Properties getColumnFamilyProperties(TableInfo tableInfo)
    {
        if (tables != null)
        {
            for (Table table : tables)
            {
                if (table != null && table.getName() != null
                        && table.getName().equalsIgnoreCase(tableInfo.getTableName()))
                {
                    return table.getProperties();
                }
            }
        }
        return null;
    }

    /**
     * Enum ColumnFamilyType for type of column family in cassandra ie Super or
     * Standard.
     * 
     */
    private enum ColumnFamilyType
    {

        /** The Standard. */
        Standard,
        /** The Super. */
        Super;

        /**
         * Gets the instance of.
         * 
         * @param type
         *            the type
         * @return the instance of
         */
        private static ColumnFamilyType getInstanceOf(String type)
        {
            if (type.equals(Type.COLUMN_FAMILY.name()))
            {
                return ColumnFamilyType.Standard;
            }
            else
            {
                return ColumnFamilyType.Super;
            }
        }
    }

    /**
     * validates entity for CounterColumnType.
     * 
     * @param clazz
     *            the clazz
     * @return true, if successful
     */
    @Override
    public boolean validateEntity(Class clazz)
    {
        EntityValidatorAgainstCounterColumn entityValidatorAgainstSchema = new EntityValidatorAgainstCounterColumn();
        return entityValidatorAgainstSchema.validateEntity(clazz);
    }

    /**
     * Sets the column family properties.
     * 
     * @param cfDef
     *            the cf def
     * @param cFProperties
     *            the c f properties
     */
    private void setColumnFamilyProperties(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        if ((cfDef != null && cFProperties != null) || (builder != null && cFProperties != null))
        {
            if (builder != null)
            {
                builder.append(CQLTranslator.WITH_CLAUSE);
            }
            onSetKeyValidation(cfDef, cFProperties, builder);

            onSetCompactionStrategy(cfDef, cFProperties, builder);

            onSetComparatorType(cfDef, cFProperties, builder);

            onSetSubComparator(cfDef, cFProperties, builder);

            onSetReplicateOnWrite(cfDef, cFProperties, builder);

            onSetCompactionThreshold(cfDef, cFProperties, builder);

            onSetComment(cfDef, cFProperties, builder);

            onSetTableId(cfDef, cFProperties, builder);

            onSetGcGrace(cfDef, cFProperties, builder);

            onSetCaching(cfDef, cFProperties, builder);

            onSetBloomFilter(cfDef, cFProperties, builder);

            onSetRepairChance(cfDef, cFProperties, builder);

            onSetReadRepairChance(cfDef, cFProperties, builder);

            // Strip last AND clause.
            if (builder != null && StringUtils.contains(builder.toString(), CQLTranslator.AND_CLAUSE))
            {
                builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
                // builder.deleteCharAt(builder.length() - 2);
            }
        }
    }

    private void onSetReadRepairChance(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String dclocalReadRepairChance = cFProperties.getProperty(CassandraConstants.DCLOCAL_READ_REPAIR_CHANCE);
        if (dclocalReadRepairChance != null)
        {
            try
            {
                if (builder != null)
                {
                    appendPropertyToBuilder(builder, dclocalReadRepairChance,
                            CassandraConstants.DCLOCAL_READ_REPAIR_CHANCE);

                }
                else
                {
                    cfDef.setDclocal_read_repair_chance(Double.parseDouble(dclocalReadRepairChance));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("READ_REPAIR_CHANCE should be double type, Caused by: .", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetRepairChance(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String readRepairChance = cFProperties.getProperty(CassandraConstants.READ_REPAIR_CHANCE);
        if (readRepairChance != null)
        {
            try
            {
                if (builder != null)
                {
                    appendPropertyToBuilder(builder, readRepairChance, CassandraConstants.READ_REPAIR_CHANCE);
                }
                else
                {
                    cfDef.setRead_repair_chance(Double.parseDouble(readRepairChance));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("READ_REPAIR_CHANCE should be double type, Caused by: .", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetBloomFilter(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String bloomFilterFpChance = cFProperties.getProperty(CassandraConstants.BLOOM_FILTER_FP_CHANCE);
        if (bloomFilterFpChance != null)
        {
            try
            {
                if (builder != null)
                {
                    appendPropertyToBuilder(builder, bloomFilterFpChance, CassandraConstants.BLOOM_FILTER_FP_CHANCE);
                }
                else
                {
                    cfDef.setBloom_filter_fp_chance(Double.parseDouble(bloomFilterFpChance));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("BLOOM_FILTER_FP_CHANCE should be double type, Caused by: .", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetCaching(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String caching = cFProperties.getProperty(CassandraConstants.CACHING);
        if (caching != null)
        {
            if (builder != null)
            {
                appendPropertyToBuilder(builder, caching, CassandraConstants.CACHING);
            }
            else
            {
                cfDef.setCaching(caching);
            }
        }
    }

    private void onSetGcGrace(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String gcGraceSeconds = cFProperties.getProperty(CassandraConstants.GC_GRACE_SECONDS);
        if (gcGraceSeconds != null)
        {
            try
            {
                if (builder != null)
                {
                    appendPropertyToBuilder(builder, gcGraceSeconds, CassandraConstants.GC_GRACE_SECONDS);
                }
                else
                {
                    cfDef.setGc_grace_seconds(Integer.parseInt(gcGraceSeconds));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("GC_GRACE_SECONDS should be numeric type, Caused by: .", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetTableId(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String id = cFProperties.getProperty(CassandraConstants.ID);
        if (id != null)
        {
            try
            {
                if (builder != null)
                {
                    // TODO::::not available with composite key?
                }
                else
                {
                    cfDef.setId(Integer.parseInt(id));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("Id should be numeric type, Caused by: ", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetComment(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String comment = cFProperties.getProperty(CassandraConstants.COMMENT);
        if (comment != null)
        {
            if (builder != null)
            {
                String comment_Str = CQLTranslator.getKeyword(CassandraConstants.COMMENT);
                builder.append(comment_Str);
                builder.append(CQLTranslator.EQ_CLAUSE);
                builder.append(CQLTranslator.QUOTE_STR);
                builder.append(comment);
                builder.append(CQLTranslator.QUOTE_STR);
                builder.append(CQLTranslator.AND_CLAUSE);

            }
            else
            {
                cfDef.setComment(comment);
            }
        }
    }

    private void onSetReplicateOnWrite(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String replicateOnWrite = cFProperties.getProperty(CassandraConstants.REPLICATE_ON_WRITE);
        if (builder != null)
        {
            appendPropertyToBuilder(builder, replicateOnWrite, CassandraConstants.REPLICATE_ON_WRITE);
        }
        else
        {
            cfDef.setReplicate_on_write(Boolean.parseBoolean(replicateOnWrite));
        }
    }

    private void onSetCompactionThreshold(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String maxCompactionThreshold = cFProperties.getProperty(CassandraConstants.MAX_COMPACTION_THRESHOLD);
        if (maxCompactionThreshold != null)
        {
            try
            {
                if (builder != null)
                {
                    // Somehow these are not working for cassandra 1.1
                    // though they claim it should work.
                    // appendPropertyToBuilder(builder,
                    // maxCompactionThreshold,
                    // CassandraConstants.MAX_COMPACTION_THRESHOLD);
                }
                else
                {
                    cfDef.setMax_compaction_threshold(Integer.parseInt(maxCompactionThreshold));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("Max_Compaction_Threshold should be numeric type, Caused by: .", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
        String minCompactionThreshold = cFProperties.getProperty(CassandraConstants.MIN_COMPACTION_THRESHOLD);
        if (minCompactionThreshold != null)
        {
            try
            {
                if (builder != null)
                {
                    // Somehow these are not working for cassandra 1.1
                    // though they claim it should work.
                    // appendPropertyToBuilder(builder,
                    // minCompactionThreshold,
                    // CassandraConstants.MIN_COMPACTION_THRESHOLD);
                }
                else
                {
                    cfDef.setMin_compaction_threshold(Integer.parseInt(minCompactionThreshold));
                }
            }
            catch (NumberFormatException nfe)
            {
                log.error("Min_Compaction_Threshold should be numeric type, Caused by: . ", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    private void onSetSubComparator(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String subComparatorType = cFProperties.getProperty(CassandraConstants.SUBCOMPARATOR_TYPE);
        if (subComparatorType != null && ColumnFamilyType.valueOf(cfDef.getColumn_type()) == ColumnFamilyType.Super)
        {
            if (builder != null)
            {
                // super column are not supported for composite key as of
                // now, leaving blank place holder..
            }
            else
            {
                cfDef.setSubcomparator_type(subComparatorType);
            }
        }
    }

    private void onSetComparatorType(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String comparatorType = cFProperties.getProperty(CassandraConstants.COMPARATOR_TYPE);
        if (comparatorType != null)
        {
            if (builder != null)
            {
                // TODO:::nothing available.
            }
            else
            {
                cfDef.setComparator_type(comparatorType);
            }
        }
    }

    private void onSetCompactionStrategy(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String compactionStrategy = cFProperties.getProperty(CassandraConstants.COMPACTION_STRATEGY);
        if (compactionStrategy != null)
        {
            if (builder != null)
            {
                String strategy_class = CQLTranslator.getKeyword(CassandraConstants.COMPACTION_STRATEGY);
                builder.append(strategy_class);
                builder.append(CQLTranslator.EQ_CLAUSE);
                builder.append(CQLTranslator.QUOTE_STR);
                builder.append(compactionStrategy);
                builder.append(CQLTranslator.QUOTE_STR);
                builder.append(CQLTranslator.AND_CLAUSE);
            }
            else
            {
                cfDef.setCompaction_strategy(compactionStrategy);
            }
        }
    }

    private void onSetKeyValidation(CfDef cfDef, Properties cFProperties, StringBuilder builder)
    {
        String keyValidationClass = cFProperties.getProperty(CassandraConstants.KEY_VALIDATION_CLASS);
        if (keyValidationClass != null)
        {
            if (builder != null)
            {
                // nothing available.
            }
            else
            {
                cfDef.setKey_validation_class(keyValidationClass);
            }
        }
    }

    /**
     * 
     * @param tableInfo
     * @return
     */
    private boolean isCql3Enabled(TableInfo tableInfo)
    {
        return containsCompositeKey(tableInfo)
                || ((cql_version != null && cql_version.equals(CassandraConstants.CQL_VERSION_3_0)) && !tableInfo
                        .getType().equals(Type.SUPER_COLUMN_FAMILY.name()));
    }

    /**
     * @param builder
     * @param replicateOnWrite
     * @param keyword
     */
    private void appendPropertyToBuilder(StringBuilder builder, String replicateOnWrite, String keyword)
    {
        String replicateOn_Write = CQLTranslator.getKeyword(keyword);
        builder.append(replicateOn_Write);
        builder.append(CQLTranslator.EQ_CLAUSE);
        builder.append(replicateOnWrite);
        builder.append(CQLTranslator.AND_CLAUSE);
    }

    /**
     * 
     * @param tableInfo
     */
    private void validateCompoundKey(TableInfo tableInfo)
    {
        if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
        {
            throw new SchemaGenerationException(
                    "Composite/Compound columns are not yet supported over Super column family by Cassandra",
                    "cassandra", databaseName);
        }
    }

    /**
     * MetaDataHandler responsible for creating column family matadata for
     * tableInfos.
     * 
     * @author Kuldeep.Mishra
     * 
     */
    private class MetaDataHandler
    {
        /**
         * get Table metadata method returns the metadata of table for given
         * tableInfo.
         * 
         * @param tableInfo
         *            the table info
         * @return the table metadata
         */
        /**
         * @param tableInfo
         * @return CfDef object
         */
        private CfDef getTableMetadata(TableInfo tableInfo)
        {
            CfDef cfDef = new CfDef();
            cfDef.setKeyspace(databaseName);
            cfDef.setName(tableInfo.getTableName());
            cfDef.setKey_validation_class(CassandraValidationClassMapper.getValidationClass(tableInfo.getTableIdType(),
                    isCql3Enabled(tableInfo)));

            Schema schema = CassandraPropertyReader.csmd.getSchema(databaseName);
            tables = schema != null ? schema.getTables() : null;

            Properties cFProperties = getColumnFamilyProperties(tableInfo);
            String defaultValidationClass = null;
            if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
            {
                getSuperColumnFamilyMetadata(tableInfo, cfDef, defaultValidationClass);
            }
            else if (tableInfo.getType() != null)
            {
                getColumnFamilyMetadata(tableInfo, cfDef, cFProperties);
            }
            setColumnFamilyProperties(cfDef, cFProperties, null);
            return cfDef;
        }

        /**
         * 
         * @param tableInfo
         * @param cfDef
         * @param defaultValidationClass
         */
        private void getSuperColumnFamilyMetadata(TableInfo tableInfo, CfDef cfDef, String defaultValidationClass)
        {
            if (isCounterColumnType(tableInfo, defaultValidationClass))
            {
                cfDef.setDefault_validation_class(CounterColumnType.class.getSimpleName());
            }
            cfDef.setColumn_type("Super");
            cfDef.setComparator_type(UTF8Type.class.getSimpleName());
            cfDef.setSubcomparator_type(UTF8Type.class.getSimpleName());
        }

        /**
         * 
         * @param tableInfo
         * @param cfDef
         * @param cFProperties
         */
        private void getColumnFamilyMetadata(TableInfo tableInfo, CfDef cfDef, Properties cFProperties)
        {
            String defaultValidationClass;
            defaultValidationClass = cFProperties != null ? cFProperties
                    .getProperty(CassandraConstants.DEFAULT_VALIDATION_CLASS) : null;
            cfDef.setColumn_type(STANDARDCOLUMNFAMILY);
            cfDef.setComparator_type(UTF8Type.class.getSimpleName());
            if (isCounterColumnType(tableInfo, defaultValidationClass))
            {
                getCounterColumnFamilyMetadata(tableInfo, cfDef);
            }
            else
            {
                List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
                List<ColumnInfo> columnInfos = tableInfo.getColumnMetadatas();
                if (columnInfos != null)
                {
                    for (ColumnInfo columnInfo : columnInfos)
                    {
                        ColumnDef columnDef = new ColumnDef();
                        if (columnInfo.isIndexable())
                        {
                            IndexInfo indexInfo = tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
                            columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
                        }
                        columnDef.setName(columnInfo.getColumnName().getBytes());
                        columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(
                                columnInfo.getType(), isCql3Enabled(tableInfo)));
                        columnDefs.add(columnDef);
                    }
                }
                cfDef.setColumn_metadata(columnDefs);
            }
        }

        /**
         * 
         * @param tableInfo
         * @param cfDef
         */
        private void getCounterColumnFamilyMetadata(TableInfo tableInfo, CfDef cfDef)
        {
            cfDef.setDefault_validation_class(CounterColumnType.class.getSimpleName());
            List<ColumnDef> counterColumnDefs = new ArrayList<ColumnDef>();
            List<ColumnInfo> columnInfos = tableInfo.getColumnMetadatas();
            if (columnInfos != null)
            {
                for (ColumnInfo columnInfo : columnInfos)
                {
                    ColumnDef columnDef = new ColumnDef();
                    if (columnInfo.isIndexable())
                    {
                        IndexInfo indexInfo = tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
                        columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
                    }
                    columnDef.setName(columnInfo.getColumnName().getBytes());
                    columnDef.setValidation_class(CounterColumnType.class.getName());
                    counterColumnDefs.add(columnDef);
                }
            }
            cfDef.setColumn_metadata(counterColumnDefs);
        }

        /**
         * Checks if is counter column type.
         * 
         * @param tableInfo
         *            the table info
         * @param defaultValidationClass
         *            the default validation class
         * @return true, if is counter column type
         */
        private boolean isCounterColumnType(TableInfo tableInfo, String defaultValidationClass)
        {
            return (csmd != null && csmd.isCounterColumn(databaseName, tableInfo.getTableName()))
                    || (defaultValidationClass != null
                            && (defaultValidationClass.equalsIgnoreCase(CounterColumnType.class.getSimpleName()) || defaultValidationClass
                                    .equalsIgnoreCase(CounterColumnType.class.getName())) || (tableInfo.getType()
                            .equals(CounterColumnType.class.getSimpleName())));
        }
    }

    /**
     * EntityValidatorAgainstCounterColumn class responsible for validating
     * classes against counter column family.
     * 
     * @author Kuldeep.Mishra
     * 
     */
    private class EntityValidatorAgainstCounterColumn
    {
        /**
         * validates entity for CounterColumnType.
         * 
         * @param clazz
         *            the clazz
         * @return true, if successful
         */
        private boolean validateEntity(Class clazz)
        {
            boolean isvalid = false;
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(clazz);
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());
            String tableName = metadata.getTableName();
            if (csmd.isCounterColumn(metadata.getSchema(), tableName))
            {
                metadata.setCounterColumnType(true);
                Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(clazz);
                if (!embeddables.isEmpty())
                {
                    isvalid = validateEmbeddedColumns(metadata, embeddables.values()) ? true : false;
                }
                else
                {
                    EntityType entity = metaModel.entity(clazz);
                    isvalid = validateColumns(metadata, entity.getAttributes()) ? true : false;
                }
                isvalid = isvalid && validateRelations(metadata) ? true : false;
            }
            else
            {
                return true;
            }
            return isvalid;
        }

        /**
         * validates entity relations if any present.
         * 
         * @param metadata
         *            the metadata
         * @return true, if successful
         */
        private boolean validateRelations(EntityMetadata metadata)
        {
            boolean isValid = true;
            for (Relation relation : metadata.getRelations())
            {
                EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation
                        .getTargetEntity());
                if (((relation.getType().equals(ForeignKey.ONE_TO_ONE) && !relation.isJoinedByPrimaryKey()) || relation
                        .getType().equals(ForeignKey.MANY_TO_MANY)) && relation.getMappedBy() == null)
                {
                    // validate Id column of target entity
                    validateColumn(targetEntityMetadata.getIdAttribute().getJavaType());
                }
                else if (relation.getType().equals(ForeignKey.ONE_TO_MANY) && relation.getMappedBy() == null)
                {
                    // if target entity is also counter column the validate
                    // source
                    // IdColumn
                    String targetTableName = targetEntityMetadata.getTableName();
                    if (csmd.isCounterColumn(targetEntityMetadata.getSchema(), targetTableName))
                    {
                        isValid = validateColumn(metadata.getIdAttribute().getJavaType()) ? true : false;
                    }
                }
            }
            return isValid;
        }

        /**
         * validate embedded column .
         * 
         * @param metadata
         * 
         * @param embeddedColumns
         *            the embedded columns
         * @return true, if successful
         */
        private boolean validateEmbeddedColumns(EntityMetadata metadata, Collection<EmbeddableType> embeddedColumns)
        {
            boolean isValid = false;
            Iterator<EmbeddableType> iter = embeddedColumns.iterator();
            while (iter.hasNext())
            {
                isValid = validateColumns(metadata, iter.next().getAttributes()) ? true : false;
            }
            return isValid;
        }

        /**
         * validate columns.
         * 
         * @param metadata
         * 
         * @param attributes
         *            the attributes
         * @return true, if successful
         */
        private boolean validateColumns(EntityMetadata metadata, Set<Attribute> attributes)
        {
            boolean isValid = true;
            for (Attribute column : attributes)
            {
                if (!metadata.getIdAttribute().equals(column) && !validateColumn(column.getJavaType()))
                {
                    isValid = false;
                    break;
                }
            }
            return isValid;
        }

        /**
         * validate a single column.
         * 
         * @param clazz
         *            the clazz
         * @return true, if successful
         */
        private boolean validateColumn(Class clazz)
        {
            boolean isValid = true;
            if (!(clazz.equals(Integer.class) || clazz.equals(int.class) || clazz.equals(Long.class) || clazz
                    .equals(long.class)))
            {
                log.warn(
                        "Default valdation class :{}, For counter column type, fields of Entity should be either long type or integer type.",
                        CounterColumnType.class.getSimpleName());
                return isValid = false;
            }
            return isValid;
        }
    }
}