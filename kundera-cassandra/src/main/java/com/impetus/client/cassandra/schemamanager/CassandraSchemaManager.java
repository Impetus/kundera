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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.kundera.Constants;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.property.PropertyAccessException;

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

    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaManager.class);

    // private static Map<String, String> dataCentersMap = new HashMap<String,
    // String>();

    private CassandraSchemaMetadata csmd = CassandraPropertyReader.csmd;

    /**
     * Instantiates a new cassandra schema manager.
     * 
     * @param client
     *            the client
     * 
     * @param clientFactory
     *            the configured client clientFactory
     */
    public CassandraSchemaManager(String clientFactory)
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
            log.error("Error occurred while creating " + databaseName + " Caused by :" + irex.getMessage());
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, irex, "Cassandra",
                    databaseName);
        }
        catch (TException tex)
        {
            log.error("Error occurred while creating " + databaseName + " Caused by :" + tex.getMessage());
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, tex, "Cassandra",
                    databaseName);
        }
        catch (SchemaDisagreementException sdex)
        {
            log.error("Error occurred while creating " + databaseName + " Caused by :" + sdex.getMessage());
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, sdex, "Cassandra",
                    databaseName);
        }
        catch (InterruptedException ie)
        {
            log.error("Error occurred while creating " + databaseName + " Caused by :" + ie.getMessage());
            throw new SchemaGenerationException("Error occurred while creating " + databaseName, ie, "Cassandra",
                    databaseName);
        }
    }

    /**
     * add tables to given keyspace {@code ksDef}.
     * 
     * @param tableInfos
     * @param ksDef
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     * @throws InterruptedException
     */
    private void addTablesToKeyspace(List<TableInfo> tableInfos, KsDef ksDef) throws InvalidRequestException,
            SchemaDisagreementException, TException, InterruptedException
    {
        cassandra_client.set_keyspace(databaseName);
        for (TableInfo tableInfo : tableInfos)
        {
            for (CfDef cfDef : ksDef.getCf_defs())
            {
                if (cfDef.getName().equalsIgnoreCase(tableInfo.getTableName()))
                // &&
                // cfDef.getColumn_type().equals(ColumnFamilyType.getInstanceOf(tableInfo.getType()).name()))
                {
                    // TimeUnit.SECONDS.sleep(5);
                    cassandra_client.system_drop_column_family(tableInfo.getTableName());
                    dropInvertedIndexTable(tableInfo);
                    TimeUnit.SECONDS.sleep(3);
                    break;
                }
            }

            cassandra_client.system_add_column_family(getTableMetadata(tableInfo));
            // Create Index Table if required
            createInvertedIndexTable(tableInfo);
        }
    }

    /**
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void createInvertedIndexTable(TableInfo tableInfo) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        boolean indexTableRequired = CassandraPropertyReader.csmd.isInvertedIndexingEnabled()
                && !tableInfo.getEmbeddedColumnMetadatas().isEmpty();
        if (indexTableRequired)
        {
            CfDef cfDef = new CfDef();
            cfDef.setKeyspace(databaseName);
            cfDef.setName(tableInfo.getTableName() + Constants.INDEX_TABLE_SUFFIX);
            cfDef.setKey_validation_class(UTF8Type.class.getSimpleName());
            cassandra_client.system_add_column_family(cfDef);
        }
    }

    /**
     * @param tableInfo
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void dropInvertedIndexTable(TableInfo tableInfo) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        boolean indexTableRequired = CassandraPropertyReader.csmd.isInvertedIndexingEnabled()
                && !tableInfo.getEmbeddedColumnMetadatas().isEmpty();
        if (indexTableRequired)
        {
            cassandra_client.system_drop_column_family(tableInfo.getTableName() + Constants.INDEX_TABLE_SUFFIX);
        }
    }

    /**
     * update method update schema and table for the list of tableInfos
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
            log.error("Error occurred while updating " + databaseName + " Caused by :" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while updating " + databaseName + e.getMessage());
            throw new SchemaGenerationException("Error occurred while updating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error occurred while updating " + databaseName + e.getMessage());
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
            log.error("Error occurred while validating " + databaseName + " Caused by:" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while validating " + databaseName + " Caused by:" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while validating " + databaseName + " Caused by:" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
    }

    /**
     * check for Tables method check the existence of schema and table
     * 
     * @param tableInfos
     *            list of TableInfos and ksDef object of KsDef
     */
    private void onValidateTables(List<TableInfo> tableInfos, KsDef ksDef)
    {
        try
        {
            cassandra_client.set_keyspace(ksDef.getName());
        }
        catch (InvalidRequestException e)
        {
            log.error("Error occurred while validating " + databaseName + " Caused by:" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        catch (TException e)
        {
            log.error("Error occurred while validating " + databaseName + " Caused by:" + e.getMessage());
            throw new SchemaGenerationException("Error occurred while validating " + databaseName, e, "Cassandra",
                    databaseName);
        }
        for (TableInfo tableInfo : tableInfos)
        {
            boolean tablefound = false;
            for (CfDef cfDef : ksDef.getCf_defs())
            {
                if (cfDef.getName().equalsIgnoreCase(tableInfo.getTableName())
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
                                // logger.error("column " +
                                // columnInfo.getColumnName()
                                // + " does not exist in column family " +
                                // tableInfo.getTableName() + "");
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
                // logger.error("column family " + tableInfo.getTableName() +
                // " does not exist in keyspace "
                // + databaseName + "");
                throw new SchemaGenerationException("Column family " + tableInfo.getTableName()
                        + " does not exist in keyspace " + databaseName + "", "Cassandra", databaseName,
                        tableInfo.getTableName());
            }
        }
    }

    /**
     * is metadata same method returns true if ColumnDef and columnInfo have
     * same metadata.
     * 
     * @param columnDef
     * @param columnInfo
     * @return
     * @throws UnsupportedEncodingException
     */
    private boolean isMetadataSame(ColumnDef columnDef, ColumnInfo columnInfo) throws UnsupportedEncodingException
    {
        // .equalsIgnoreCase(CassandraValidationClassMapper.getValidationClass(columnInfo
        // .getType())
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
     */
    private void updateTables(List<TableInfo> tableInfos, KsDef ksDef) throws InvalidRequestException, TException,
            SchemaDisagreementException
    {

        cassandra_client.set_keyspace(databaseName);
        for (TableInfo tableInfo : tableInfos)
        {
            boolean found = false;
            for (CfDef cfDef : ksDef.getCf_defs())
            {
                if (cfDef.getName().equalsIgnoreCase(tableInfo.getTableName())
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
                    // cassandra_client.system_drop_column_family(tableInfo.getTableName());
                    // cassandra_client.system_add_column_family(getTableMetadata(tableInfo));
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                cassandra_client.system_add_column_family(getTableMetadata(tableInfo));
            }
        }
    }

    /**
     * isInedexesPresent method return whether indexes present or not on
     * particular column.
     * 
     * @param columnInfo
     * @param cfDef
     * @return
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
     * @return
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
     * of tableInfos
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    private void createKeyspaceAndTables(List<TableInfo> tableInfos)
    {
        KsDef ksDef = new KsDef(databaseName, csmd.getPlacement_strategy(), null);
        Map<String, String> strategy_options = new HashMap<String, String>();
        if (csmd.getPlacement_strategy().equalsIgnoreCase(SimpleStrategy.class.getName())
                || csmd.getPlacement_strategy().equalsIgnoreCase(SimpleStrategy.class.getSimpleName()))
        {
            strategy_options.put("replication_factor", csmd.getReplication_factor());
        }
        else
        {
            for (String dataCeneteName : csmd.getDataCenters().keySet())
            {
                strategy_options.put(dataCeneteName, csmd.getDataCenters().get(dataCeneteName));
            }
        }
        ksDef.setStrategy_options(strategy_options);
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        for (TableInfo tableInfo : tableInfos)
        {
            cfDefs.add(getTableMetadata(tableInfo));
        }
        ksDef.setCf_defs(cfDefs);
        try
        {
            createKeyspace(ksDef);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while creating schema in cassandra, Caused by:" + e.getMessage());
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error while creating schema in cassandra, Caused by:" + e.getMessage());
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }
        catch (TException e)
        {
            log.error("Error while creating schema in cassandra, Caused by:" + e.getMessage());
            throw new SchemaGenerationException(e, "Cassandra", databaseName);
        }

    }

    /**
     * create keyspace method create keyspace for given ksDef.
     * 
     * @param ksDef
     *            a Object of KsDef.
     */
    private void createKeyspace(KsDef ksDef) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        cassandra_client.system_add_keyspace(ksDef);
    }

    /**
     * get Table metadata method returns the metadata of table for given
     * tableInfo
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

        if (tableInfo.getType() != null && tableInfo.getType().equals(Type.COLUMN_FAMILY.name()))

        {
            cfDef.setColumn_type("Standard");
            if (csmd.isCounterColumn(tableInfo.getTableName()))
            {
                cfDef.setDefault_validation_class(CounterColumnType.class.getSimpleName());
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
                            columnDef.setIndex_type(IndexType.KEYS);
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
        else if (tableInfo.getType() != null)
        {
            if (csmd.isCounterColumn(tableInfo.getTableName()))
            {
                cfDef.setDefault_validation_class(CounterColumnType.class.getSimpleName());
            }
            cfDef.setColumn_type("Super");
        }
        return cfDef;
    }

    /**
     * Enum ColumnFamilyType for type of column family in cassandra ie Super or
     * Standard.
     * 
     */
    private enum ColumnFamilyType
    {
        Standard, Super;
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
                    cassandra_client.system_drop_column_family(tableInfo.getTableName());
                }
            }
            catch (InvalidRequestException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by:" + e.getMessage());
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (TException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by:" + e.getMessage());
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (SchemaDisagreementException e)
            {
                log.error("Error during dropping schema in cassandra, Caused by:" + e.getMessage());
                throw new SchemaGenerationException(e, "Cassandra");
            }
        }
        cassandra_client = null;
    }

    /**
     * initiate client method initiates the client.
     * 
     * @return boolean value ie client started or not.
     * 
     */
    protected boolean initiateClient()
    {
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
                log.error("Error while opening socket , Caused by:" + e.getMessage());
                throw new SchemaGenerationException(e, "Cassandra");
            }
            catch (NumberFormatException e)
            {
                log.error("Error during creating schema in cassandra, Caused by:" + e.getMessage());
                throw new SchemaGenerationException(e, "Cassandra");
            }
            return true;
        }
        return false;
    }

    /**
     * validates entity for CounterColumnType.
     */
    @Override
    public boolean validateEntity(Class clazz)
    {
        boolean isvalid = false;
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(clazz);
        String tableName = metadata.getTableName();
        if (csmd.isCounterColumn(tableName))
        {
            metadata.setCounterColumnType(true);
            List<EmbeddedColumn> embeddedColumns = metadata.getEmbeddedColumnsAsList();
            if (!embeddedColumns.isEmpty())
            {
                isvalid = validateEmbeddedColumns(embeddedColumns) ? true : false;
            }
            else
            {
                isvalid = validateColumns(metadata.getColumnsAsList()) ? true : false;
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
                validateColumn(targetEntityMetadata.getIdColumn());
            }
            else if (relation.getType().equals(ForeignKey.ONE_TO_MANY) && relation.getMappedBy() == null)
            {
                // if target entity is also counter column the validate source
                // IdColumn
                String targetTableName = targetEntityMetadata.getTableName();
                if (csmd.isCounterColumn(targetTableName))
                {
                    isValid = validateColumn(metadata.getIdColumn()) ? true : false;
                }
            }
        }
        return isValid;
    }

    /**
     * validate embedded column .
     * 
     * @param embeddedColumns
     */
    private boolean validateEmbeddedColumns(List<EmbeddedColumn> embeddedColumns)
    {
        boolean isValid = false;
        for (EmbeddedColumn embeddedColumn : embeddedColumns)
        {
            isValid = validateColumns(embeddedColumn.getColumns()) ? true : false;
        }
        return isValid;
    }

    /**
     * validate columns.
     * 
     * @param columns
     */
    private boolean validateColumns(List<Column> columns)
    {
        boolean isValid = true;
        for (Column column : columns)
        {
            if (!validateColumn(column))
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
     * @param column
     */
    private boolean validateColumn(Column column)
    {
        boolean isValid = true;
        if (!(column.getField().getType().equals(Integer.class) || column.getField().getType().equals(int.class)
                || column.getField().getType().equals(Long.class) || column.getField().getType().equals(long.class)))
        {
            log.warn("Default valdation class :" + CounterColumnType.class.getSimpleName()
                    + ", For counter column type, fields of Entity should be either long type or integer type");
            return isValid = false;
        }
        return isValid;
    }
}