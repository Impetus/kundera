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
import java.util.concurrent.TimeUnit;

import javax.persistence.Embeddable;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexType;
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

    /**
     * Cassandra client variable holds the client.
     */
    private Cassandra.Client cassandra_client;

    private String cql_version = CassandraConstants.CQL_VERSION_2_0;

    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaManager.class);

    // private static Map<String, String> dataCentersMap = new HashMap<String,
    // String>();

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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#exportSchema
     * ()
     */
    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema()
    {
        cql_version = externalProperties != null ? (String) externalProperties.get(CassandraConstants.CQL_VERSION)
                : CassandraConstants.CQL_VERSION_3_0;
        super.exportSchema();
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
                    if (tableInfo.getTableIdType() != null
                            && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
                    {
                        dropTableUsingCQL(tableInfo);
                    }
                    else
                    {
                        cassandra_client.system_drop_column_family(tableInfo.getTableName());
                    }
                }
            }
            catch (InvalidRequestException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by: ", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (TException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by: ", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (SchemaDisagreementException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by: ", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
        }
        cassandra_client = null;
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
            KsDef ksDef = cassandra_client.describe_keyspace(databaseName);

            addTablesToKeyspace(tableInfos, ksDef);
        }
        catch (NotFoundException nfex)
        {
            createKeyspaceAndTables(tableInfos);

        }
        catch (InvalidRequestException irex)
        {
            log.error("Error occurred while creating " + databaseName + ", Caused by: ", irex);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, irex, "Cassandra",
                    databaseName);
        }
        catch (TException tex)
        {
            log.error("Error occurred while creating " + databaseName + " Caused by: ", tex);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, tex, "Cassandra",
                    databaseName);
        }
        catch (SchemaDisagreementException sdex)
        {
            log.error("Error occurred while creating " + databaseName + ", Caused by:", sdex);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, sdex, "Cassandra",
                    databaseName);
        }
        catch (InterruptedException ie)
        {
            log.error("Error occurred while creating " + databaseName + ", Caused by: ", ie);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, ie, "Cassandra",
                    databaseName);
        }
        catch (UnavailableException ue)
        {
            log.error("Error occurred while creating " + databaseName + ", Caused by: ", ue);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, ue, "Cassandra",
                    databaseName);
        }
        catch (TimedOutException toe)
        {
            log.error("Error occurred while creating " + databaseName + ", Caused by: ", toe);
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, toe, "Cassandra",
                    databaseName);
        }
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
            KsDef ksDef = cassandra_client.describe_keyspace(databaseName);
            updateTables(tableInfos, ksDef);
        }
        catch (NotFoundException e)
        {
            createKeyspaceAndTables(tableInfos);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while updating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while updating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error occurred while updating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (UnavailableException e)
        {
            log.error("Error occurred while updating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TimedOutException e)
        {
            log.error("Error occurred while updating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
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
        catch (NotFoundException e)
        {
            log.error("Error occurred while validating " + databaseName + ", Caused by:", e);
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while validating " + databaseName + ", Caused by:", e);
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while validating " + databaseName + ", Caused by:", e);
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
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
        if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
        {
            log.error("Host or port should not be null, Port should be numeric.");
            throw new IllegalArgumentException("Host or port should not be null, Port should be numeric.");
        }

        if (cassandra_client == null)
        {
            TSocket socket = new TSocket(host, Integer.parseInt(port));
            TTransport transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            cassandra_client = new Cassandra.Client(protocol);
            try
            {
                if (!socket.isOpen())
                {
                    socket.open();
                }
            }
            catch (TTransportException e)
            {
                log.error("Error while opening socket, Caused by: ", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (NumberFormatException e)
            {
                log.error("Error during creating schema in cassandra, Caused by: ", e);
                throw new SchemaGenerationException(e, "Cassandra");
            }
        }
        return cassandra_client != null ? true : false;
    }

    /**
     * add tables to given keyspace {@code ksDef}.
     * 
     * @param tableInfos
     *            the table infos
     * @param ksDef
     *            the ks def
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
     * @throws InterruptedException
     *             the interrupted exception
     * @throws TimedOutException
     * @throws UnavailableException
     */

    private void addTablesToKeyspace(List<TableInfo> tableInfos, KsDef ksDef) throws InvalidRequestException,
            SchemaDisagreementException, TException, InterruptedException, UnavailableException, TimedOutException
    {
        cassandra_client.set_keyspace(databaseName);
        for (TableInfo tableInfo : tableInfos)
        {
            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForCQL3();
            }
            for (CfDef cfDef : ksDef.getCf_defs())
            {
                if (cfDef.getName().equals(tableInfo.getTableName()))
                {
                    cassandra_client.system_drop_column_family(tableInfo.getTableName());
                    dropInvertedIndexTable(tableInfo);
                    TimeUnit.SECONDS.sleep(2);
                    break;
                }
            }

            if (tableInfo.getTableIdType() != null && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
            {
                if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
                {
                    throw new SchemaGenerationException(
                            "Composite/Compound columns are yet supported over Super column family by Cassandra",
                            "cassandra", databaseName);
                }
                else
                {
                    onCompoundKey(tableInfo);
                }
            }
            else
            {
                cassandra_client.system_add_column_family(getTableMetadata(tableInfo));
                // Create Index Table if required
                createInvertedIndexTable(tableInfo);
            }

            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForThrift();
            }
        }
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
    private void onCompoundKey(TableInfo tableInfo)
    {
        CQLTranslator translator = new CQLTranslator();
        String columnFamilyQuery = CQLTranslator.CREATE_COLUMNFAMILY_QUERY;
        columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableInfo.getTableName()).toString());

        List<ColumnInfo> columns = tableInfo.getColumnMetadatas();

        StringBuilder queryBuilder = new StringBuilder();

        // for normal columns
        onCompositeColumns(translator, columns, queryBuilder);

        // ideally it will always be one as more super column families
        // are not allowed with compound/composite key.

        List<EmbeddedColumnInfo> compositeColumns = tableInfo.getEmbeddedColumnMetadatas();
        EmbeddableType compoEmbeddableType = compositeColumns.get(0).getEmbeddable();

        // for composite columns
        onCompositeColumns(translator, compositeColumns.get(0).getColumns(), queryBuilder);

        // strip last ",".
        if (queryBuilder.length() > 0)
        {
            queryBuilder.deleteCharAt(queryBuilder.length() - 1);

            columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMNS, queryBuilder.toString());
            queryBuilder = new StringBuilder(columnFamilyQuery);
        }

        // append primary key clause

        queryBuilder.append(translator.ADD_PRIMARYKEY_CLAUSE);

        Field[] fields = tableInfo.getTableIdType().getDeclaredFields();

        StringBuilder primaryKeyBuilder = new StringBuilder();

        // To ensure field ordering
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                Attribute attribute = compoEmbeddableType.getAttribute(f.getName());
                translator.appendColumnName(primaryKeyBuilder, ((AbstractAttribute) attribute).getJPAColumnName());
                primaryKeyBuilder.append(" ,");
            }
        }

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
        catch (UnsupportedEncodingException e)
        {
            log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(), e,
                    "Cassandra", databaseName);
        }
        catch (UnavailableException e)
        {
            log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(), e,
                    "Cassandra", databaseName);
        }
        catch (TimedOutException e)
        {
            log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(), e,
                    "Cassandra", databaseName);
        }
        catch (InvalidRequestException ire)
        {
            StringBuilder builder = new StringBuilder("Cannot add already existing column family ");
//            translator.ensureCase(builder, databaseName);
//            builder.append(" to keyspace ");
//            translator.ensureCase(builder, tableInfo.getTableName());
            if (ire.getWhy() != null && ire.getWhy().contains(builder.toString()) && operation.equalsIgnoreCase(ScheamOperationType.update.name()))
            {
                for (ColumnInfo column : tableInfo.getColumnMetadatas())
                {
                    addColumnToTable(tableInfo, translator, column);
                }
            }
            else if (ire.getWhy() != null && ire.getWhy().equals(builder.toString()))
            {
                // First drop existing column family.
                dropTableUsingCQL(tableInfo);

                // And create new column family.
                onCompoundKey(tableInfo);
            }
            else
            {
                log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", ire);
                throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(),
                        ire, "Cassandra", databaseName);
            }
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(), e,
                    "Cassandra", databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while creating table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(), e,
                    "Cassandra", databaseName);
        }
        
        // After successful  schema operation, perform index creation.
        createIndex(tableInfo);
    }

    /**
     * Create secondary indexes on columns.
     * 
     * @param tableInfo
     */
    private void createIndex(TableInfo tableInfo)
    {
        StringBuilder indexQueryBuilder = new StringBuilder("create index $COLUMN_NAME on \"");
        indexQueryBuilder.append(tableInfo.getTableName());
        indexQueryBuilder.append( "\"(\"$COLUMN_NAME\")");
        for (IndexInfo indexInfo : tableInfo.getColumnsToBeIndexed())
        {
            String replacedWithindexName  = StringUtils.replace(indexQueryBuilder.toString(), "$COLUMN_NAME", indexInfo.getColumnName());
            try
            {
                cassandra_client.execute_cql3_query(ByteBuffer.wrap(replacedWithindexName.getBytes()), Compression.NONE,
                        ConsistencyLevel.ONE);
            }
            catch (InvalidRequestException ire)
            {
                if (ire.getWhy() != null && !ire.getWhy().equals("Index already exists") && operation.equalsIgnoreCase(ScheamOperationType.update.name()))
                {
                    onLogException(tableInfo, indexInfo, ire);
                }
            }
            catch (UnavailableException uex)
            {
                onLogException(tableInfo, indexInfo, uex);
            }
            catch (TimedOutException toex)
            {
                onLogException(tableInfo, indexInfo, toex);
            }
            catch (SchemaDisagreementException sdex)
            {
                onLogException(tableInfo, indexInfo, sdex);
            }
            catch (TException tex)
            {
                onLogException(tableInfo, indexInfo, tex);
            }
        }
    }

    private void onLogException(TableInfo tableInfo, IndexInfo indexInfo, Exception ire)
    {
        log.error("Error occurred while creating indexes on column "+ indexInfo.getColumnName() + " of table " + tableInfo.getTableName() + ", Caused by: ", ire);
        throw new SchemaGenerationException("Error occurred while creating indexes on column "+ indexInfo.getColumnName() + " of table " + tableInfo.getTableName(), ire,
                "Cassandra", databaseName);
    }

    /**
     * Drop table using cql3.
     * 
     * @param tableInfo
     */
    private void dropTableUsingCQL(TableInfo tableInfo)
    {
        CQLTranslator translator = new CQLTranslator();
        StringBuilder deleteQuery = new StringBuilder("drop table ");
        translator.ensureCase(deleteQuery, tableInfo.getTableName());
        try
        {
            cassandra_client.execute_cql3_query(ByteBuffer.wrap(deleteQuery.toString().getBytes()), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException ire)
        {
            log.error("Error occurred while droping table " + tableInfo.getTableName() + ", Caused by: ", ire);
            throw new SchemaGenerationException("Error occurred while droping table " + tableInfo.getTableName(), ire,
                    "Cassandra", databaseName);
        }
        catch (UnavailableException ue)
        {
            log.error("Error occurred while droping table " + tableInfo.getTableName() + ", Caused by: ", ue);
            throw new SchemaGenerationException("Error occurred while droping table " + tableInfo.getTableName(), ue,
                    "Cassandra", databaseName);
        }
        catch (TimedOutException toe)
        {
            log.error("Error occurred while droping table " + tableInfo.getTableName() + ", Caused by: ", toe);
            throw new SchemaGenerationException("Error occurred while droping table " + tableInfo.getTableName(), toe,
                    "Cassandra", databaseName);
        }
        catch (SchemaDisagreementException sde)
        {
            log.error("Error occurred while droping table " + tableInfo.getTableName() + ", Caused by: ", sde);
            throw new SchemaGenerationException("Error occurred while droping table " + tableInfo.getTableName(), sde,
                    "Cassandra", databaseName);
        }
        catch (TException te)
        {
            log.error("Error occurred while droping table " + tableInfo.getTableName() + ", Caused by: ", te);
            throw new SchemaGenerationException("Error occurred while droping table " + tableInfo.getTableName(), te,
                    "Cassandra", databaseName);
        }
    }

    /**
     * Adds column to table if not exists previously ie alter table.
     * 
     * @param tableInfo
     * @param translator
     * @param column
     */
    private void addColumnToTable(TableInfo tableInfo, CQLTranslator translator, ColumnInfo column)
    {
        StringBuilder addColumnQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(addColumnQuery, tableInfo.getTableName());
        addColumnQuery.append(" ADD ");
        translator.ensureCase(addColumnQuery, column.getColumnName());
        addColumnQuery.append(" "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType())));
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
                log.error("Error occurred while altering column type of  table " + tableInfo.getTableName()
                        + ", Caused by: ", ireforAddColumn);
                throw new SchemaGenerationException("Error occurred while adding column into table "
                        + tableInfo.getTableName(), ireforAddColumn, "Cassandra", databaseName);
            }
        }
        catch (UnavailableException e)
        {
            log.error("Error occurred while altering column type of  table " + tableInfo.getTableName()
                    + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while adding column into table "
                    + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (TimedOutException e)
        {
            log.error("Error occurred while adding column into table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while adding column into table "
                    + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error occurred while adding column into table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while adding column into table "
                    + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while adding column into table " + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while adding column into table "
                    + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
    }

    /**
     * Alters column type of an existing column.
     * 
     * @param tableInfo
     * @param translator
     * @param column
     */
    private void alterColumnType(TableInfo tableInfo, CQLTranslator translator, ColumnInfo column)
    {
        StringBuilder alterColumnTypeQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(alterColumnTypeQuery, tableInfo.getTableName());
        alterColumnTypeQuery.append(" ALTER ");
        translator.ensureCase(alterColumnTypeQuery, column.getColumnName());
        alterColumnTypeQuery.append(" TYPE "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType())));
        try
        {
            cassandra_client.execute_cql3_query(ByteBuffer.wrap(alterColumnTypeQuery.toString().getBytes()),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while altering column type of column " + column.getColumnName() + " of   table "
                    + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while altering column type of column "
                    + column.getColumnName() + " of   table " + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (UnavailableException e)
        {
            log.error("Error occurred while altering column type of column " + column.getColumnName() + " of   table "
                    + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while altering column type of column "
                    + column.getColumnName() + " of   table " + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (TimedOutException e)
        {
            log.error("Error occurred while altering column type of column " + column.getColumnName() + " of   table "
                    + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while altering column type of column "
                    + column.getColumnName() + " of   table " + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error occurred while altering column type of column " + column.getColumnName() + " of   table "
                    + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while altering column type of column "
                    + column.getColumnName() + " of   table " + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while altering column type of column " + column.getColumnName() + " of   table "
                    + tableInfo.getTableName() + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while altering column type of column "
                    + column.getColumnName() + " of   table " + tableInfo.getTableName(), e, "Cassandra", databaseName);
        }
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
    private void onCompositeColumns(CQLTranslator translator, List<ColumnInfo> columns, StringBuilder queryBuilder)
    {
        for (ColumnInfo colInfo : columns)
        {
            String dataType = CassandraValidationClassMapper.getValidationClass(colInfo.getType());
            String cqlType = translator.getCQLType(dataType);
            translator.appendColumnName(queryBuilder, colInfo.getColumnName(), cqlType);
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
            catch (InvalidRequestException e)
            {
                if (log.isInfoEnabled())
                    log.info("Error while dropping inverted index table, Caused by: ", e);
            }
            catch (SchemaDisagreementException e)
            {
                if (log.isInfoEnabled())
                    log.info("Error while dropping inverted index table, Caused by: ", e);
            }
            catch (TException e)
            {
                if (log.isInfoEnabled())
                    log.info("Error while dropping inverted index table, Caused by: ", e);
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
     */
    private void onValidateTables(List<TableInfo> tableInfos, KsDef ksDef)
    {
        try
        {
            cassandra_client.set_keyspace(ksDef.getName());
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while validating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while validating " + databaseName + ", Caused by: ", e);
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        for (TableInfo tableInfo : tableInfos)
        {
            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForCQL3();
            }
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
                            boolean columnfound = false;
                            for (ColumnDef columnDef : cfDef.getColumn_metadata())
                            {
                                try
                                {
                                    if (isMetadataSame(columnDef, columnInfo))
                                    {
                                        columnfound = true;
                                        break;
                                    }
                                }
                                catch (UnsupportedEncodingException e)
                                {
                                    throw new PropertyAccessException(e);
                                }
                            }
                            if (!columnfound)
                            {
                                throw new SchemaGenerationException("Column " + columnInfo.getColumnName()
                                        + " does not exist in column family " + tableInfo.getTableName() + "",
                                        "Cassandra", databaseName, tableInfo.getTableName());
                            }
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

            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForThrift();
            }
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
    private boolean isMetadataSame(ColumnDef columnDef, ColumnInfo columnInfo) throws UnsupportedEncodingException
    {
        return (new String(columnDef.getName(), Constants.ENCODING).equals(columnInfo.getColumnName()))
                && (columnDef.isSetIndex_type() == columnInfo.isIndexable() || (columnDef.isSetIndex_type())) ? (columnDef
                .getValidation_class()
                .endsWith(CassandraValidationClassMapper.getValidationClass(columnInfo.getType()))) : false;
    }

    /**
     * add tables to Keyspace method add the table to given keyspace.
     * 
     * @param tableInfos
     *            list of TableInfos and ksDef object of KsDef.
     * @param ksDef
     *            the ks def
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws TException
     *             the t exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TimedOutException
     * @throws UnavailableException
     */
    private void updateTables(List<TableInfo> tableInfos, KsDef ksDef) throws InvalidRequestException, TException,
            SchemaDisagreementException, UnavailableException, TimedOutException
    {
        cassandra_client.set_keyspace(databaseName);
        for (TableInfo tableInfo : tableInfos)
        {
            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForCQL3();
            }

            boolean found = false;
            for (CfDef cfDef : ksDef.getCf_defs())
            {
                if (cfDef.getName().equals(tableInfo.getTableName())
                        && cfDef.getColumn_type().equals(ColumnFamilyType.getInstanceOf(tableInfo.getType()).name()))
                {
                    if (cfDef.getColumn_type().equalsIgnoreCase("Standard"))
                    {

                        for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                        {
                            if (!isIndexesPresent(columnInfo, cfDef))
                            {
                                cfDef.addToColumn_metadata(getColumnMetadata(columnInfo));
                            }
                        }
                    }
                    cassandra_client.system_update_column_family(cfDef);
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                if (tableInfo.getTableIdType() != null
                        && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
                {
                    if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
                    {
                        throw new SchemaGenerationException(
                                "Composite/Compound columns are yet supported over Super column family by Cassandra",
                                "cassandra", databaseName);
                    }
                    else
                    {
                        onCompoundKey(tableInfo);
                    }
                }
                else
                {
                    cassandra_client.system_add_column_family(getTableMetadata(tableInfo));
                    // Create Index Table if required
                    createInvertedIndexTable(tableInfo);
                }
            }
            if (isCQL3Enabled(tableInfo))
            {
                CassandraValidationClassMapper.resetMapperForThrift();
            }
        }
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
     */
    private boolean isIndexesPresent(ColumnInfo columnInfo, CfDef cfDef)
    {
        for (ColumnDef columnDef : cfDef.getColumn_metadata())
        {
            try
            {
                if (new String(columnDef.getName(), Constants.ENCODING).equals(columnInfo.getColumnName()))
                {
                    if (columnDef.getIndex_type() != null)
                    {
                        return true;
                    }
                }
            }
            catch (UnsupportedEncodingException ueex)
            {
                throw new PropertyAccessException(ueex);
            }
        }
        return false;
    }

    /**
     * getColumnMetadata use for getting column metadata for specific
     * columnInfo.
     * 
     * @param columnInfo
     *            the column info
     * @return the column metadata
     */
    private ColumnDef getColumnMetadata(ColumnInfo columnInfo)
    {
        ColumnDef columnDef = new ColumnDef();
        columnDef.setName(columnInfo.getColumnName().getBytes());
        columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(columnInfo.getType()));
        if (columnInfo.isIndexable())
        {
            columnDef.setIndex_type(IndexType.KEYS);
        }
        return columnDef;
    }

    /**
     * create keyspace and table method create keyspace and table for the list
     * of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     * @throws SchemaDisagreementException
     * @throws TException
     * @throws InvalidRequestException
     */
    private void createKeyspaceAndTables(List<TableInfo> tableInfos)
    {
        KsDef ksDef = new KsDef(databaseName, csmd.getPlacement_strategy(databaseName), null);
        Map<String, String> strategy_options = new HashMap<String, String>();
        setProperties(ksDef, strategy_options);
        try
        {
            ksDef.setStrategy_options(strategy_options);
            List<CfDef> cfDefs = new ArrayList<CfDef>();
            List<TableInfo> compoundColumnFamilies = new ArrayList<TableInfo>();
            for (TableInfo tableInfo : tableInfos)
            {
                if (isCQL3Enabled(tableInfo))
                {
                    CassandraValidationClassMapper.resetMapperForCQL3();
                }
                if ((tableInfo.getTableIdType() != null && !tableInfo.getTableIdType().isAnnotationPresent(
                        Embeddable.class))
                        || tableInfo.getTableIdType() == null)
                {
                    cfDefs.add(getTableMetadata(tableInfo));
                    CfDef cfDef = getInvertedIndexCF(tableInfo);
                    if (cfDef != null)
                        cfDefs.add(getInvertedIndexCF(tableInfo));
                }
                else if (tableInfo.getTableIdType() != null
                        && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
                {
                    compoundColumnFamilies.add(tableInfo);
                }

                if (isCQL3Enabled(tableInfo))
                {
                    CassandraValidationClassMapper.resetMapperForThrift();
                }
            }

            ksDef.setCf_defs(cfDefs);
            createKeyspace(ksDef);

            for (TableInfo tableInfo : compoundColumnFamilies)
            {
                if (isCQL3Enabled(tableInfo))
                {
                    CassandraValidationClassMapper.resetMapperForCQL3();
                }
                cassandra_client.set_keyspace(databaseName);
                onCompoundKey(tableInfo);
                if (isCQL3Enabled(tableInfo))
                {
                    CassandraValidationClassMapper.resetMapperForThrift();
                }
            }

            // Recreate Inverted Index Table if applicable
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while creating schema in cassandra, Caused by: ", e);
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error while creating schema in cassandra, Caused by: ", e);
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }
        catch (TException e)
        {
            log.error("Error while creating schema in cassandra, Caused by: ", e);
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }

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
     * create keyspace method create keyspace for given ksDef.
     * 
     * @param ksDef
     *            a Object of KsDef.
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
     */
    private void createKeyspace(KsDef ksDef) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        cassandra_client.system_add_keyspace(ksDef);
    }

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
        cfDef.setKey_validation_class(CassandraValidationClassMapper.getValidationClass(tableInfo.getTableIdType()));

        Schema schema = CassandraPropertyReader.csmd.getSchema(databaseName);
        tables = schema != null ? schema.getTables() : null;

        Properties cFProperties = getColumnFamilyProperties(tableInfo);
        String defaultValidationClass = null;
        if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
        {
            if (isCounterColumnType(tableInfo, defaultValidationClass))
            {
                cfDef.setDefault_validation_class(CounterColumnType.class.getSimpleName());
            }
            cfDef.setColumn_type("Super");
            cfDef.setComparator_type(UTF8Type.class.getSimpleName());
            cfDef.setSubcomparator_type(UTF8Type.class.getSimpleName());
        }
        else if (tableInfo.getType() != null)
        {
            defaultValidationClass = cFProperties != null ? cFProperties
                    .getProperty(CassandraConstants.DEFAULT_VALIDATION_CLASS) : null;
            cfDef.setColumn_type("Standard");
            cfDef.setComparator_type(UTF8Type.class.getSimpleName());
            if (isCounterColumnType(tableInfo, defaultValidationClass))
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
                        columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(columnInfo
                                .getType()));
                        columnDefs.add(columnDef);
                    }
                }
                cfDef.setColumn_metadata(columnDefs);
            }
        }
        setColumnFamilyProperties(cfDef, cFProperties, null);
        return cfDef;
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
            EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
            if (((relation.getType().equals(ForeignKey.ONE_TO_ONE) && !relation.isJoinedByPrimaryKey()) || relation
                    .getType().equals(ForeignKey.MANY_TO_MANY)) && relation.getMappedBy() == null)
            {
                // validate Id column of target entity
                validateColumn(targetEntityMetadata.getIdAttribute().getJavaType());
            }
            else if (relation.getType().equals(ForeignKey.ONE_TO_MANY) && relation.getMappedBy() == null)
            {
                // if target entity is also counter column the validate source
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
        // for (EmbeddedColumn embeddedColumn : embeddedColumns)
        // {
        // isValid = validateColumns(embeddedColumn.getColumns()) ? true :
        // false;
        // }
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
            log.warn("Default valdation class :" + CounterColumnType.class.getSimpleName()
                    + ", For counter column type, fields of Entity should be either long type or integer type.");
            return isValid = false;
        }
        return isValid;
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
            String replicateOnWrite = cFProperties.getProperty(CassandraConstants.REPLICATE_ON_WRITE);
            if (builder != null)
            {
                appendPropertyToBuilder(builder, replicateOnWrite, CassandraConstants.REPLICATE_ON_WRITE);
            }
            else
            {
                cfDef.setReplicate_on_write(Boolean.parseBoolean(replicateOnWrite));
            }
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
                    log.error("Max_Compaction_Threshold should be numeric type, Caused by: ", nfe);
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
                    log.error("Min_Compaction_Threshold should be numeric type, Caused by: ", nfe);
                    throw new SchemaGenerationException(nfe);
                }
            }

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
                    log.error("GC_GRACE_SECONDS should be numeric type, Caused by: ", nfe);
                    throw new SchemaGenerationException(nfe);
                }
            }

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
                    log.error("BLOOM_FILTER_FP_CHANCE should be double type, Caused by: ", nfe);
                    throw new SchemaGenerationException(nfe);
                }
            }
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
                    log.error("READ_REPAIR_CHANCE should be double type, Caused by: ", nfe);
                    throw new SchemaGenerationException(nfe);
                }
            }
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
                    log.error("READ_REPAIR_CHANCE should be double type, Caused by: ", nfe);
                    throw new SchemaGenerationException(nfe);
                }
            }

            // Strip last AND clause.
            if (builder != null && StringUtils.contains(builder.toString(), CQLTranslator.AND_CLAUSE))
            {
                builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
                // builder.deleteCharAt(builder.length() - 2);
            }
        }
    }

    private boolean isCQL3Enabled(TableInfo tableInfo)
    {
        return tableInfo.getTableIdType() != null
                && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class)
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
}