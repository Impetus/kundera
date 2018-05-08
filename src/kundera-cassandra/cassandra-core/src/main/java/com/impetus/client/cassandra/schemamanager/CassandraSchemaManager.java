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
import java.nio.charset.CharacterCodingException;
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
import javax.persistence.metamodel.Type.PersistenceType;

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
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
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
import com.impetus.kundera.KunderaException;
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
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Manages auto schema operation defined in {@code ScheamOperationType}.
 * 
 * @author Kuldeep.kumar
 * 
 */
public class CassandraSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    /** The Constant STANDARDCOLUMNFAMILY. */
    private static final String STANDARDCOLUMNFAMILY = "Standard";

    /** Cassandra client variable holds the client. */
    private Cassandra.Client cassandra_client;

    /** The cql_version. */
    private String cql_version = CassandraConstants.CQL_VERSION_2_0;

    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaManager.class);

    /** The csmd. */
    private CassandraSchemaMetadata csmd = CassandraPropertyReader.csmd;

    /** The tables. */
    private List<Table> tables;

    /** The created keyspaces. */
    private List<String> createdKeyspaces = new ArrayList<String>();

    /** The created userTypes. */
    private List<String> createdPuEmbeddables = new ArrayList<String>();

    /**
     * Instantiates a new cassandra schema manager.
     * 
     * @param clientFactory
     *            the configured client clientFactory
     * @param puProperties
     *            the pu properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CassandraSchemaManager(String clientFactory, Map<String, Object> puProperties,
            final KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, puProperties, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#exportSchema
     * (java.lang.String, java.util.List)
     */
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
                dropKeyspaceOrCFs();
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
     * Drop keyspace or c fs.
     * 
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
     * @throws Exception
     *             the exception
     */
    private void dropKeyspaceOrCFs() throws InvalidRequestException, SchemaDisagreementException, TException, Exception
    {
        if (createdKeyspaces.contains(databaseName))// drop if created during
                                                    // create-drop call.
        {
            cassandra_client.system_drop_keyspace(databaseName);
        }
        else
        {
            cassandra_client.set_keyspace(databaseName);
            for (TableInfo tableInfo : tableInfos)
            {
                dropColumnFamily(tableInfo);
            }
        }
    }

    /**
     * Drops column family specified in table info.
     * 
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
     */
    private void dropColumnFamily(TableInfo tableInfo) throws Exception
    {
        if (isCql3Enabled(tableInfo))
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
            createOrUpdateKeyspace(tableInfos);
        }
        catch (Exception ex)
        {
            throw new SchemaGenerationException(ex);
        }
    }

    /**
     * Creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     * @throws Exception
     *             the exception
     */
    private void createOrUpdateKeyspace(List<TableInfo> tableInfos) throws Exception
    {
        KsDef ksDef = onCreateKeyspace(); // create keyspace event.
        createColumnFamilies(tableInfos, ksDef); // create column family event.
    }

    /**
     * On create keyspace.
     * 
     * @return the ks def
     * @throws Exception
     *             the exception
     */
    private KsDef onCreateKeyspace() throws Exception
    {
        try
        {
            createdKeyspaces.add(databaseName);
            createKeyspace();
        }
        catch (InvalidRequestException irex)
        {
            // Ignore and add a log.debug
            // keyspace already exists.
            // remove from list if already created.
            createdKeyspaces.remove(databaseName);
        }
        cassandra_client.set_keyspace(databaseName);
        return cassandra_client.describe_keyspace(databaseName);
    }

    /**
     * Creates keyspace.
     * 
     * @throws Exception
     *             the exception
     */
    private void createKeyspace() throws Exception
    {
        if (cql_version != null && cql_version.equals(CassandraConstants.CQL_VERSION_3_0))
        {
            onCql3CreateKeyspace();
        }
        else
        {
            Map<String, String> strategy_options = new HashMap<String, String>();
            List<CfDef> cfDefs = new ArrayList<CfDef>();
            KsDef ksDef = new KsDef(databaseName, csmd.getPlacement_strategy(databaseName), cfDefs);
            setProperties(ksDef, strategy_options);
            ksDef.setStrategy_options(strategy_options);
            cassandra_client.system_add_keyspace(ksDef);
        }
    }

    /**
     * On cql3 create keyspace.
     * 
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
     * @throws UnsupportedEncodingException
     *             the unsupported encoding exception
     */
    private void onCql3CreateKeyspace() throws InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException, TException, UnsupportedEncodingException
    {
        String createKeyspace = CQLTranslator.CREATE_KEYSPACE;
        String placement_strategy = csmd.getPlacement_strategy(databaseName);
        String replication_conf = CQLTranslator.SIMPLE_REPLICATION;
        createKeyspace = createKeyspace.replace("$KEYSPACE", Constants.ESCAPE_QUOTE + databaseName
                + Constants.ESCAPE_QUOTE);

        Schema schema = CassandraPropertyReader.csmd.getSchema(databaseName);

        if (schema != null && schema.getName() != null && schema.getName().equalsIgnoreCase(databaseName)
                && schema.getSchemaProperties() != null)
        {
            Properties schemaProperties = schema.getSchemaProperties();
            if (placement_strategy.equalsIgnoreCase(SimpleStrategy.class.getSimpleName())
                    || placement_strategy.equalsIgnoreCase(SimpleStrategy.class.getName()))
            {
                String replicationFactor = schemaProperties.getProperty(CassandraConstants.REPLICATION_FACTOR,
                        CassandraConstants.DEFAULT_REPLICATION_FACTOR);

                replication_conf = replication_conf.replace("$REPLICATION_FACTOR", replicationFactor);
                createKeyspace = createKeyspace.replace("$CLASS", placement_strategy);
            }
            else if (placement_strategy.equalsIgnoreCase(NetworkTopologyStrategy.class.getSimpleName())
                    || placement_strategy.equalsIgnoreCase(NetworkTopologyStrategy.class.getName()))
            {

                if (schema.getDataCenters() != null && !schema.getDataCenters().isEmpty())
                {
                    StringBuilder builder = new StringBuilder();

                    for (DataCenter dc : schema.getDataCenters())
                    {
                        builder.append(CQLTranslator.QUOTE_STR);
                        builder.append(dc.getName());
                        builder.append(CQLTranslator.QUOTE_STR);
                        builder.append(":");
                        builder.append(dc.getValue());
                        builder.append(CQLTranslator.COMMA_STR);
                    }

                    builder.delete(builder.lastIndexOf(CQLTranslator.COMMA_STR), builder.length());

                    replication_conf = builder.toString();
                }
            }

            createKeyspace = createKeyspace.replace("$CLASS", placement_strategy);
            createKeyspace = createKeyspace.replace("$REPLICATION", replication_conf);

            boolean isDurableWrites = Boolean.parseBoolean(schemaProperties.getProperty(
                    CassandraConstants.DURABLE_WRITES, "true"));
            createKeyspace = createKeyspace.replace("$DURABLE_WRITES", isDurableWrites + "");
        }
        else
        {
            createKeyspace = createKeyspace.replace("$CLASS", placement_strategy);
            replication_conf = replication_conf.replace("$REPLICATION_FACTOR",
                    (CharSequence) externalProperties.getOrDefault(CassandraConstants.REPLICATION_FACTOR,
                    		CassandraConstants.DEFAULT_REPLICATION_FACTOR));
            createKeyspace = createKeyspace.replace("$REPLICATION", replication_conf);
            createKeyspace = createKeyspace.replace("$DURABLE_WRITES", "true");
        }
        cassandra_client.execute_cql3_query(ByteBuffer.wrap(createKeyspace.getBytes(Constants.CHARSET_UTF8)),
                Compression.NONE, ConsistencyLevel.ONE);
        KunderaCoreUtils.printQuery(createKeyspace, showQuery);
    }

    /**
     * Creates the column families.
     * 
     * @param tableInfos
     *            the table infos
     * @param ksDef
     *            the ks def
     * @throws Exception
     *             the exception
     */
    private void createColumnFamilies(List<TableInfo> tableInfos, KsDef ksDef) throws Exception
    {
        for (TableInfo tableInfo : tableInfos)
        {
            if (isCql3Enabled(tableInfo))
            {
                createOrUpdateUsingCQL3(tableInfo, ksDef);
                createIndexUsingCql(tableInfo);
            }
            else
            {
                createOrUpdateColumnFamily(tableInfo, ksDef);
            }

            // Create Inverted Indexed Table if required.
            createInvertedIndexTable(tableInfo, ksDef);
        }
    }

    /**
     * Creates the or update column family.
     * 
     * @param tableInfo
     *            the table info
     * @param ksDef
     *            the ks def
     * @throws Exception
     *             the exception
     */
    private void createOrUpdateColumnFamily(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        MetaDataHandler handler = new MetaDataHandler();

        if (containsCompositeKey(tableInfo))
        {
            validateCompoundKey(tableInfo);
            createOrUpdateUsingCQL3(tableInfo, ksDef);

            // After successful schema operation, perform index creation.
            createIndexUsingCql(tableInfo);
        }
        else if (containsCollectionColumns(tableInfo) || isCql3Enabled(tableInfo))
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
     * Contains collection columns.
     * 
     * @param tableInfo
     *            the table info
     * @return true, if successful
     */
    private boolean containsCollectionColumns(TableInfo tableInfo)
    {
        return !tableInfo.getCollectionColumnMetadatas().isEmpty();
    }

    /**
     * Contains embedded columns.
     * 
     * @param tableInfo
     *            the table info
     * @return true, if successful
     */
    private boolean containsEmbeddedColumns(TableInfo tableInfo)
    {
        return !tableInfo.getEmbeddedColumnMetadatas().isEmpty();
    }

    /**
     * Contains element collection columns.
     * 
     * @param tableInfo
     *            the table info
     * @return true, if successful
     */
    private boolean containsElementCollectionColumns(TableInfo tableInfo)
    {
        return !tableInfo.getElementCollectionMetadatas().isEmpty();
    }

    /**
     * Contains composite key.
     * 
     * @param tableInfo
     *            the table info
     * @return true, if successful
     */
    private boolean containsCompositeKey(TableInfo tableInfo)
    {
        return tableInfo.getTableIdType() != null && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class);
    }

    /**
     * Update existing column family.
     * 
     * @param tableInfo
     *            the table info
     * @param ksDef
     *            the ks def
     * @param irex
     *            the irex
     * @throws Exception
     *             the exception
     */
    private void updateExistingColumnFamily(TableInfo tableInfo, KsDef ksDef, InvalidRequestException irex)
            throws Exception
    {
        StringBuilder builder = new StringBuilder("^Cannot add already existing (?:column family|table) .*$");

        if (irex.getWhy() != null && irex.getWhy().matches(builder.toString()))
        {
            SchemaOperationType operationType = SchemaOperationType.getInstance(operation);
            switch (operationType)
            {
            case create:
                handleCreate(tableInfo, ksDef);
                break;

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
                    createIndexUsingCql(tableInfo);
                }
                else
                {
                    updateTable(ksDef, tableInfo);
                }
                break;

            default:
                break;
            }
        }
        else
        {
            log.error("Error occurred while creating table {}, Caused by: {}.", tableInfo.getTableName(), irex);
            throw new SchemaGenerationException("Error occurred while creating table " + tableInfo.getTableName(),
                    irex, "Cassandra", databaseName);
        }
    }

    /**
     * Handle create.
     * 
     * @param tableInfo
     *            the table info
     * @param ksDef
     *            the ks def
     * @throws Exception
     *             the exception
     */
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

    /**
     * On drop.
     * 
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
     */
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
            createOrUpdateKeyspace(tableInfos);
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
            int thriftPort = externalProperties.get(CassandraConstants.THRIFT_PORT) != null ? Integer
                    .parseInt((String) externalProperties.get(CassandraConstants.THRIFT_PORT)) : Integer.parseInt(port);
            TSocket socket = new TSocket(host, thriftPort);
            TTransport transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport, true, true);
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
     * Creates (or updates) a column family definition using CQL 3 Should
     * replace onCompoundKey.
     * 
     * @param tableInfo
     *            the table info
     * @param ksDef
     *            the ks def
     * @throws Exception
     *             the exception
     */
    private void createOrUpdateUsingCQL3(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        try
        {

            cassandra_client.set_cql_version(CassandraConstants.CQL_VERSION_3_0);
            cassandra_client.set_keyspace(databaseName);
            MetaDataHandler handler = new MetaDataHandler();
            CQLTranslator translator = new CQLTranslator();
            String columnFamilyQuery = CQLTranslator.CREATE_COLUMNFAMILY_QUERY;
            columnFamilyQuery = StringUtils.replace(columnFamilyQuery, CQLTranslator.COLUMN_FAMILY, translator
                    .ensureCase(new StringBuilder(), tableInfo.getTableName(), false).toString());

            List<ColumnInfo> columns = tableInfo.getColumnMetadatas();

            Properties cfProperties = getColumnFamilyProperties(tableInfo);

            String defaultValidationClass = cfProperties != null ? cfProperties
                    .getProperty(CassandraConstants.DEFAULT_VALIDATION_CLASS) : null;

            StringBuilder queryBuilder = new StringBuilder();

            // For normal columns
            boolean isCounterColumnType = isCounterColumnType(tableInfo, defaultValidationClass);
            onCompositeColumns(translator, columns, queryBuilder, null, isCounterColumnType);
            onCollectionColumns(translator, tableInfo.getCollectionColumnMetadatas(), queryBuilder);

            // ideally it will always be one as more super column families
            // are not allowed with compound/composite key.
            List<EmbeddedColumnInfo> compositeColumns = tableInfo.getEmbeddedColumnMetadatas();
            EmbeddableType compoEmbeddableType = null;

            if (!compositeColumns.isEmpty() && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
            {
                EmbeddedColumnInfo compositeId = null;
                for (EmbeddedColumnInfo compositeCol : compositeColumns)
                {
                    if (compositeCol.getEmbeddedColumnName().equals(tableInfo.getIdColumnName()))
                    {
                        compositeId = compositeCol;
                        break;
                    }
                }
                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                        puMetadata.getPersistenceUnitName());
                // get list of names of all embeddables in compositeid
                List compositeEmbeddables = new ArrayList<String>();
                getCompositeIdEmbeddables(compositeId.getEmbeddable(), compositeEmbeddables, metaModel);

                if (compositeColumns.size() > 1 || !tableInfo.getElementCollectionMetadatas().isEmpty())
                {
                    createTypeforEmbeddables(compositeEmbeddables);
                    onEmbeddedColumns(translator, tableInfo, queryBuilder, compositeEmbeddables);
                    onElementCollectionColumns(translator, tableInfo.getElementCollectionMetadatas(), queryBuilder);

                }

                compoEmbeddableType = compositeId.getEmbeddable();
                onCompositeColumns(translator, compositeId.getColumns(), queryBuilder, columns, isCounterColumnType);
            }
            else
            {
                if (!compositeColumns.isEmpty() || !tableInfo.getElementCollectionMetadatas().isEmpty())
                {
                    // embedded create udts
                    // check for multiple embedded and collections in embedded
                    // entity
                    createTypeforEmbeddables(new ArrayList<String>());
                    onEmbeddedColumns(translator, tableInfo, queryBuilder, new ArrayList<String>());
                    onElementCollectionColumns(translator, tableInfo.getElementCollectionMetadatas(), queryBuilder);

                }
                String dataType = CassandraValidationClassMapper.getValidationClass(tableInfo.getTableIdType(), true);
                String cqlType = translator.getCQLType(dataType);
                translator.appendColumnName(queryBuilder, tableInfo.getIdColumnName(), cqlType);
                queryBuilder.append(Constants.SPACE_COMMA);
            }

            queryBuilder = replaceColumnsAndStripLastChar(columnFamilyQuery, queryBuilder);

            // append primary key clause
            queryBuilder.append(translator.ADD_PRIMARYKEY_CLAUSE);

            // To ensure field ordering
            // check if embedded is also an id
            if (compoEmbeddableType != null && tableInfo.getTableIdType().isAnnotationPresent(Embeddable.class))
            {
                Field[] fields = tableInfo.getTableIdType().getDeclaredFields();
                StringBuilder primaryKeyBuilder = new StringBuilder();
                appendPrimaryKey(translator, compoEmbeddableType, fields, primaryKeyBuilder);
                // should not be null.
                primaryKeyBuilder.deleteCharAt(primaryKeyBuilder.length() - 1);
                queryBuilder = new StringBuilder(StringUtils.replace(queryBuilder.toString(), CQLTranslator.COLUMNS,
                        primaryKeyBuilder.toString()));

                StringBuilder clusterKeyOrderingBuilder = new StringBuilder();
                appendClusteringOrder(translator, compositeColumns.get(0).getColumns(), clusterKeyOrderingBuilder,
                        primaryKeyBuilder);
                if (clusterKeyOrderingBuilder.length() != 0)
                {
                    // append cluster key order clause
                    queryBuilder.append(translator.CREATE_COLUMNFAMILY_CLUSTER_ORDER.replace(CQLTranslator.COLUMNS,
                            clusterKeyOrderingBuilder.toString()));
                }
            }
            else
            {
                queryBuilder = new StringBuilder(StringUtils.replace(queryBuilder.toString(), CQLTranslator.COLUMNS,
                        Constants.ESCAPE_QUOTE + tableInfo.getIdColumnName() + Constants.ESCAPE_QUOTE));
            }

            // set column family properties defined in configuration
            // property/xml
            // files.
            setColumnFamilyProperties(null, cfProperties, queryBuilder);
            KunderaCoreUtils.printQuery(queryBuilder.toString(), showQuery);

            cassandra_client.execute_cql3_query(
                    ByteBuffer.wrap(queryBuilder.toString().getBytes(Constants.CHARSET_UTF8)), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException irex)
        {
            updateExistingColumnFamily(tableInfo, ksDef, irex);
        }

    }

    /**
     * Gets the composite id embeddables.
     * 
     * @param embeddable
     *            the embeddable
     * @param compositeEmbeddables
     *            the composite embeddables
     * @param metaModel
     *            the meta model
     * @return the composite id embeddables
     */
    private void getCompositeIdEmbeddables(EmbeddableType embeddable, List compositeEmbeddables, MetamodelImpl metaModel)
    {
        compositeEmbeddables.add(embeddable.getJavaType().getSimpleName());

        for (Object column : embeddable.getAttributes())
        {

            Attribute columnAttribute = (Attribute) column;
            Field f = (Field) columnAttribute.getJavaMember();

            if (columnAttribute.getJavaType().isAnnotationPresent(Embeddable.class))
            {
                getCompositeIdEmbeddables(metaModel.embeddable(columnAttribute.getJavaType()), compositeEmbeddables,
                        metaModel);
            }
        }
    }

    /**
     * On element collection columns.
     * 
     * @param translator
     *            the translator
     * @param collectionColumnInfos
     *            the collection column infos
     * @param queryBuilder
     *            the query builder
     */
    private void onElementCollectionColumns(CQLTranslator translator, List<CollectionColumnInfo> collectionColumnInfos,
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
                    genericTypesBuilder.append(Constants.STR_LT);
                    if (genericClasses.get(0).getAnnotation(Embeddable.class) != null)
                    {
                        String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                                + genericClasses.get(0).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                        genericTypesBuilder.append(frozenKey);
                    }
                    else
                    {
                        String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0),
                                true);
                        genericTypesBuilder.append(translator.getCQLType(keyDataType));
                    }
                    genericTypesBuilder.append(Constants.SPACE_COMMA);
                    if (genericClasses.get(1).getAnnotation(Embeddable.class) != null)
                    {
                        String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                                + genericClasses.get(1).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                        genericTypesBuilder.append(frozenKey);
                    }
                    else
                    {
                        String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(1),
                                true);
                        genericTypesBuilder.append(translator.getCQLType(keyDataType));
                    }
                    genericTypesBuilder.append(Constants.STR_GT);
                }
                else if ((ListType.class.getSimpleName().equals(dataType) || SetType.class.getSimpleName().equals(
                        dataType))
                        && genericClasses.size() == 1)
                {
                    genericTypesBuilder.append(Constants.STR_LT);
                    if (genericClasses.get(0).getAnnotation(Embeddable.class) != null)
                    {
                        String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                                + genericClasses.get(0).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                        genericTypesBuilder.append(frozenKey);
                    }
                    else
                    {
                        String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0),
                                true);
                        genericTypesBuilder.append(translator.getCQLType(keyDataType));
                    }
                    genericTypesBuilder.append(Constants.STR_GT);
                }
                else
                {
                    throw new SchemaGenerationException("Incorrect collection field definition for "
                            + cci.getCollectionColumnName() + ". Generic Types must be defined correctly.");
                }
            }

            if (genericTypesBuilder != null)
            {
                collectionCqlType += genericTypesBuilder.toString();
            }

            translator.appendColumnName(queryBuilder, collectionColumnName, collectionCqlType);
            queryBuilder.append(Constants.SPACE_COMMA);

        }

    }

    /**
     * On embedded columns.
     * 
     * @param translator
     *            the translator
     * @param tableInfo
     *            the table info
     * @param queryBuilder
     *            the query builder
     * @param compositeEmbeddables
     *            the composite embeddables
     */
    private void onEmbeddedColumns(CQLTranslator translator, TableInfo tableInfo, StringBuilder queryBuilder,
            List compositeEmbeddables)
    {
        List<EmbeddedColumnInfo> embeddedColumns = tableInfo.getEmbeddedColumnMetadatas();
        for (EmbeddedColumnInfo embColInfo : embeddedColumns)
        {
            if (!compositeEmbeddables.contains(embColInfo.getEmbeddable().getJavaType().getSimpleName()))
            {
                String cqlType = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                        + embColInfo.getEmbeddable().getJavaType().getSimpleName() + Constants.ESCAPE_QUOTE
                        + Constants.STR_GT + translator.COMMA_STR;
                translator.appendColumnName(queryBuilder, embColInfo.getEmbeddedColumnName(), cqlType);
            }
        }
    }

    /**
     * Creates the typefor embeddables.
     * 
     * @param compositeEmbeddables
     *            the composite embeddables
     */
    private void createTypeforEmbeddables(List compositeEmbeddables)
    {
        if (!createdPuEmbeddables.contains(puMetadata.getPersistenceUnitName()))
        {
            CQLTranslator translator = new CQLTranslator();

            Map<String, String> embNametoUDTQuery = new HashMap<String, String>();
            Map<String, List<String>> embNametoDependentList = new HashMap<String, List<String>>();

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    puMetadata.getPersistenceUnitName());

            Iterator iter = metaModel.getEmbeddables().iterator();
            while (iter.hasNext())
            {
                List<String> childEmb = new ArrayList<String>();

                String typeQuery = CQLTranslator.CREATE_TYPE;
                EmbeddableType embeddedColumn = (EmbeddableType) iter.next();
                if (!embeddedColumn.getPersistenceType().equals(PersistenceType.EMBEDDABLE)
                        || compositeEmbeddables.contains(embeddedColumn.getJavaType().getSimpleName()))
                {
                    continue;
                }

                typeQuery = StringUtils.replace(typeQuery, CQLTranslator.TYPE,
                        translator.ensureCase(new StringBuilder(), embeddedColumn.getJavaType().getSimpleName(), false)
                                .toString());

                StringBuilder typeQueryBuilder = new StringBuilder();

                for (Object column : embeddedColumn.getAttributes())
                {

                    Attribute columnAttribute = (Attribute) column;
                    Field f = (Field) columnAttribute.getJavaMember();

                    if (columnAttribute.getJavaType().isAnnotationPresent(Embeddable.class))
                    {
                        // handle embeddable
                        String cqlType = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                                + columnAttribute.getJavaType().getSimpleName() + Constants.ESCAPE_QUOTE
                                + Constants.STR_GT;
                        translator.appendColumnName(typeQueryBuilder, columnAttribute.getName(), cqlType);
                        typeQueryBuilder.append(Constants.SPACE_COMMA);
                        childEmb.add(columnAttribute.getJavaType().getSimpleName());
                    }
                    else if (columnAttribute.isCollection())
                    {
                        // handle element collection with embeddables
                        handleElementCollectionAttribute(translator, columnAttribute, typeQueryBuilder);
                        if (!MetadataUtils.isBasicElementCollectionField((Field) columnAttribute.getJavaMember()))
                        {
                            childEmb.add(((AbstractAttribute) columnAttribute).getBindableJavaType().getSimpleName());
                        }

                    }
                    else
                    {
                        String cqlType = null;
                        String dataType = CassandraValidationClassMapper.getValidationClass(f.getType(), true);
                        cqlType = translator.getCQLType(dataType);
                        // check for JPA names
                        translator.appendColumnName(typeQueryBuilder,
                                ((AbstractAttribute) columnAttribute).getJPAColumnName(), cqlType);
                        typeQueryBuilder.append(Constants.SPACE_COMMA);

                    }

                }
                typeQueryBuilder = replaceColumnsAndStripLastChar(typeQuery, typeQueryBuilder);
                typeQueryBuilder.append(CQLTranslator.CLOSE_BRACKET);
                embNametoUDTQuery.put(embeddedColumn.getJavaType().getSimpleName(), typeQueryBuilder.toString());
                embNametoDependentList.put(embeddedColumn.getJavaType().getSimpleName(), childEmb);
                // run query final

            }
            postProcessEmbedded(embNametoUDTQuery, embNametoDependentList);
            createdPuEmbeddables.add(puMetadata.getPersistenceUnitName());
        }
    }

    /**
     * Handle element collection attribute.
     * 
     * @param translator
     *            the translator
     * @param attribute
     *            the attribute
     * @param typeQueryBuilder
     *            the type query builder
     */
    private void handleElementCollectionAttribute(CQLTranslator translator, Attribute attribute,
            StringBuilder typeQueryBuilder)
    {
        String dataType = CassandraValidationClassMapper.getValidationClass(attribute.getJavaType(), true);

        // CQL Type of collection column
        String collectionCqlType = translator.getCQLType(dataType);

        // Collection Column Name
        String collectionColumnName = new String(((AbstractAttribute) attribute).getJPAColumnName());

        // Generic Type list
        StringBuilder genericTypesBuilder = null;
        List<Class<?>> genericClasses = PropertyAccessorHelper.getGenericClasses((Field) attribute.getJavaMember());
        if (!genericClasses.isEmpty())
        {
            genericTypesBuilder = new StringBuilder();
            if (MapType.class.getSimpleName().equals(dataType) && genericClasses.size() == 2)
            {
                genericTypesBuilder.append(Constants.STR_LT);
                if (genericClasses.get(0).getAnnotation(Embeddable.class) != null)
                {
                    String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                            + genericClasses.get(0).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                    genericTypesBuilder.append(frozenKey);
                }
                else
                {
                    String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0), true);
                    genericTypesBuilder.append(translator.getCQLType(keyDataType));
                }
                genericTypesBuilder.append(Constants.SPACE_COMMA);
                if (genericClasses.get(1).getAnnotation(Embeddable.class) != null)
                {
                    String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                            + genericClasses.get(1).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                    genericTypesBuilder.append(frozenKey);
                }
                else
                {
                    String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(1), true);
                    genericTypesBuilder.append(translator.getCQLType(keyDataType));
                }
                genericTypesBuilder.append(Constants.STR_GT);
            }
            else if ((ListType.class.getSimpleName().equals(dataType) || SetType.class.getSimpleName().equals(dataType))
                    && genericClasses.size() == 1)
            {
                genericTypesBuilder.append(Constants.STR_LT);
                if (genericClasses.get(0).getAnnotation(Embeddable.class) != null)
                {
                    String frozenKey = CQLTranslator.FROZEN + Constants.STR_LT + Constants.ESCAPE_QUOTE
                            + genericClasses.get(0).getSimpleName() + Constants.ESCAPE_QUOTE + Constants.STR_GT;
                    genericTypesBuilder.append(frozenKey);
                }
                else
                {
                    String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0), true);
                    genericTypesBuilder.append(translator.getCQLType(keyDataType));
                }
                genericTypesBuilder.append(Constants.STR_GT);
            }
            else
            {
                throw new SchemaGenerationException("Incorrect collection field definition for "
                        + ((AbstractAttribute) attribute).getJPAColumnName()
                        + ". Generic Types must be defined correctly.");
            }
        }

        if (genericTypesBuilder != null)
        {
            collectionCqlType += genericTypesBuilder.toString();
        }

        translator.appendColumnName(typeQueryBuilder, collectionColumnName, collectionCqlType);
        typeQueryBuilder.append(Constants.SPACE_COMMA);

    }

    /**
     * Post process embedded.
     * 
     * @param embNametoUDTQuery
     *            the emb nameto udt query
     * @param embNametoDependentList
     *            the emb nameto dependent list
     * 
     */
    private void postProcessEmbedded(Map<String, String> embNametoUDTQuery,
            Map<String, List<String>> embNametoDependentList)
    {
        for (Map.Entry<String, List<String>> entry : embNametoDependentList.entrySet())
        {
            checkRelationAndExecuteQuery(entry.getKey(), embNametoDependentList, embNametoUDTQuery);
        }
    }

    /**
     * Check relation and execute query.
     * 
     * @param embeddableKey
     *            the embeddable key
     * @param embeddableToDependentEmbeddables
     *            the embeddable to dependent embeddables
     * @param queries
     *            the queries
     * 
     */
    private void checkRelationAndExecuteQuery(String embeddableKey,
            Map<String, List<String>> embeddableToDependentEmbeddables, Map<String, String> queries)
    {
        List<String> dependentEmbeddables = embeddableToDependentEmbeddables.get(embeddableKey);

        if (!dependentEmbeddables.isEmpty())
        {
            for (String dependentEmbeddable : dependentEmbeddables)
            {
                checkRelationAndExecuteQuery(dependentEmbeddable, embeddableToDependentEmbeddables, queries);
            }
        }
        KunderaCoreUtils.printQuery(queries.get(embeddableKey), showQuery);

        try
        {
            cassandra_client.execute_cql3_query(
                    ByteBuffer.wrap(queries.get(embeddableKey).getBytes(Constants.CHARSET_UTF8)), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            throw new KunderaException("Error while creating type: " + queries.get(embeddableKey), e);
        }

    }

    /**
     * Append primary key.
     * 
     * @param translator
     *            the translator
     * @param compoEmbeddableType
     *            the compo embeddable type
     * @param fields
     *            the fields
     * @param queryBuilder
     *            the query builder
     */
    private void appendPrimaryKey(CQLTranslator translator, EmbeddableType compoEmbeddableType, Field[] fields,
            StringBuilder queryBuilder)
    {
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                if (f.getType().isAnnotationPresent(Embeddable.class))
                { // compound partition key
                    MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                            puMetadata.getPersistenceUnitName());
                    queryBuilder.append(translator.OPEN_BRACKET);
                    queryBuilder.append(translator.SPACE_STRING);
                    appendPrimaryKey(translator, (EmbeddableType) metaModel.embeddable(f.getType()), f.getType()
                            .getDeclaredFields(), queryBuilder);
                    queryBuilder.deleteCharAt(queryBuilder.length() - 1);
                    queryBuilder.append(translator.CLOSE_BRACKET);
                    queryBuilder.append(Constants.SPACE_COMMA);

                }
                else
                {
                    Attribute attribute = compoEmbeddableType.getAttribute(f.getName());
                    translator.appendColumnName(queryBuilder, ((AbstractAttribute) attribute).getJPAColumnName());
                    queryBuilder.append(Constants.SPACE_COMMA);
                }
            }
        }
    }

    /**
     * Append clustering order.
     * 
     * @param translator
     *            the translator
     * @param compositeColumns
     *            the composite columns
     * @param clusterKeyOrderingBuilder
     *            the cluster key ordering builder
     * @param primaryKeyBuilder
     *            the primary key builder
     */
    private void appendClusteringOrder(CQLTranslator translator, List<ColumnInfo> compositeColumns,
            StringBuilder clusterKeyOrderingBuilder, StringBuilder primaryKeyBuilder)
    {
        // to retrieve the order in which cluster key is formed
        String[] primaryKeys = primaryKeyBuilder.toString().split("\\s*,\\s*");
        for (String primaryKey : primaryKeys)
        {
            // to compare the objects without enclosing quotes
            primaryKey = primaryKey.trim().substring(1, primaryKey.trim().length() - 1);
            for (ColumnInfo colInfo : compositeColumns)
            {

                if (primaryKey.equals(colInfo.getColumnName()))
                {
                    if (colInfo.getOrderBy() != null)
                    {
                        translator.appendColumnName(clusterKeyOrderingBuilder, colInfo.getColumnName());
                        clusterKeyOrderingBuilder.append(translator.SPACE_STRING);
                        clusterKeyOrderingBuilder.append(colInfo.getOrderBy());
                        clusterKeyOrderingBuilder.append(translator.COMMA_STR);
                    }

                }
            }
        }
        if (clusterKeyOrderingBuilder.length() != 0)
        {
            clusterKeyOrderingBuilder.deleteCharAt(clusterKeyOrderingBuilder.toString().lastIndexOf(","));
            clusterKeyOrderingBuilder.append(translator.CLOSE_BRACKET);
        }

    }

    /**
     * Strip last char.
     * 
     * @param columnFamilyQuery
     *            the column family query
     * @param queryBuilder
     *            the query builder
     * @return the string builder
     */
    private StringBuilder replaceColumnsAndStripLastChar(String columnFamilyQuery, StringBuilder queryBuilder)
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

    /**
     * Creates the index using thrift.
     * 
     * @param tableInfo
     *            the table info
     * @param cfDef
     *            the cf def
     * @throws Exception
     *             the exception
     */
    private void createIndexUsingThrift(TableInfo tableInfo, CfDef cfDef) throws Exception
    {
        for (IndexInfo indexInfo : tableInfo.getColumnsToBeIndexed())
        {
            for (ColumnDef columnDef : cfDef.getColumn_metadata())
            {
                if (new String(columnDef.getName(), Constants.ENCODING).equals(indexInfo.getColumnName()))
                {
                    columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
                    // columnDef.setIndex_name(indexInfo.getIndexName());
                }
            }
        }
        cassandra_client.system_update_column_family(cfDef);
    }

    /**
     * Create secondary indexes on columns.
     * 
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
     */
    private void createIndexUsingCql(TableInfo tableInfo) throws Exception
    {

        List<String> embeddedIndexes = new ArrayList<String>();
        for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
        {
            for (ColumnInfo columnInfo : embeddedColumnInfo.getColumns())
            {
                if (columnInfo.isIndexable())
                {
                    embeddedIndexes.add(columnInfo.getColumnName());
                }
            }
        }

        StringBuilder indexQueryBuilder = new StringBuilder("create index if not exists on \"");
        indexQueryBuilder.append(tableInfo.getTableName());
        indexQueryBuilder.append("\"(\"$COLUMN_NAME\")");
        tableInfo.getColumnsToBeIndexed();
        for (IndexInfo indexInfo : tableInfo.getColumnsToBeIndexed())
        {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(indexInfo.getColumnName());

            // indexes on embeddables not supported in cql3
            if (!embeddedIndexes.contains(indexInfo.getColumnName()))
            {
                String replacedWithindexName = StringUtils.replace(indexQueryBuilder.toString(), "$COLUMN_NAME",
                        indexInfo.getColumnName());

                try
                {
                    KunderaCoreUtils.printQuery(replacedWithindexName, showQuery);
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
                                + indexInfo.getColumnName() + " of table " + tableInfo.getTableName(), ire,
                                "Cassandra", databaseName);
                    }
                }
            }

        }
    }

    /**
     * Drops table using cql3.
     * 
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
     */
    private void dropTableUsingCql(TableInfo tableInfo) throws Exception
    {

        CQLTranslator translator = new CQLTranslator();
        StringBuilder dropQuery = new StringBuilder("drop table ");
        translator.ensureCase(dropQuery, tableInfo.getTableName(), false);

        KunderaCoreUtils.printQuery(dropQuery.toString(), showQuery);

        cassandra_client.execute_cql3_query(ByteBuffer.wrap(dropQuery.toString().getBytes()), Compression.NONE,
                ConsistencyLevel.ONE);
    }

    /**
     * Adds column to table if not exists previously i.e. alter table.
     * 
     * @param tableInfo
     *            the table info
     * @param column
     *            the column
     * @throws Exception
     *             the exception
     */
    private void addColumnToTable(TableInfo tableInfo, ColumnInfo column) throws Exception
    {
        CQLTranslator translator = new CQLTranslator();
        StringBuilder addColumnQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(addColumnQuery, tableInfo.getTableName(), false);
        addColumnQuery.append(" ADD ");
        translator.ensureCase(addColumnQuery, column.getColumnName(), false);
        addColumnQuery.append(" "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType(),
                        isCql3Enabled(tableInfo))));
        try
        {
            KunderaCoreUtils.printQuery(addColumnQuery.toString(), showQuery);
            cassandra_client.execute_cql3_query(ByteBuffer.wrap(addColumnQuery.toString().getBytes()),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException ireforAddColumn)
        {
            StringBuilder ireforAddColumnbBuilder = new StringBuilder("Invalid column name ");
            ireforAddColumnbBuilder.append(column.getColumnName() + " because it conflicts with an existing column");
            if (ireforAddColumn.getWhy() != null && ireforAddColumn.getWhy().equals(ireforAddColumnbBuilder.toString()))
            {
                // alterColumnType(tableInfo, translator, column);
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
     * showSchema Alters column type of an existing column.
     * 
     * @param tableInfo
     *            the table info
     * @param translator
     *            the translator
     * @param column
     *            the column
     * @throws Exception
     *             the exception
     */
    private void alterColumnType(TableInfo tableInfo, CQLTranslator translator, ColumnInfo column) throws Exception
    {
        StringBuilder alterColumnTypeQuery = new StringBuilder("ALTER TABLE ");
        translator.ensureCase(alterColumnTypeQuery, tableInfo.getTableName(), false);
        alterColumnTypeQuery.append(" ALTER ");
        translator.ensureCase(alterColumnTypeQuery, column.getColumnName(), false);
        alterColumnTypeQuery.append(" TYPE "
                + translator.getCQLType(CassandraValidationClassMapper.getValidationClass(column.getType(),
                        isCql3Enabled(tableInfo))));
        cassandra_client.execute_cql3_query(ByteBuffer.wrap(alterColumnTypeQuery.toString().getBytes()),
                Compression.NONE, ConsistencyLevel.ONE);

        KunderaCoreUtils.printQuery(alterColumnTypeQuery.toString(), showQuery);
    }

    /**
     * On composite columns.
     * 
     * @param translator
     *            the translator
     * @param compositeColumns
     *            the composite columns
     * @param queryBuilder
     *            the query builder
     * @param columns
     *            the columns
     * @param isCounterColumnFamily
     *            the is counter column family
     */
    private void onCompositeColumns(CQLTranslator translator, List<ColumnInfo> compositeColumns,
            StringBuilder queryBuilder, List<ColumnInfo> columns, boolean isCounterColumnFamily)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                puMetadata.getPersistenceUnitName());
        for (ColumnInfo colInfo : compositeColumns)
        {
            if (columns == null || (columns != null && !columns.contains(colInfo)))
            {
                String cqlType = null;
                if (isCounterColumnFamily)
                {
                    cqlType = "counter";
                    translator.appendColumnName(queryBuilder, colInfo.getColumnName(), cqlType);
                    queryBuilder.append(Constants.SPACE_COMMA);
                }
                // check for composite partition keys #734
                else if (colInfo.getType().isAnnotationPresent(Embeddable.class))
                {
                    EmbeddableType embeddedObject = (EmbeddableType) metaModel.embeddable(colInfo.getType());
                    for (Field embeddedColumn : colInfo.getType().getDeclaredFields())
                    {
                        if (!ReflectUtils.isTransientOrStatic(embeddedColumn))
                        {
                            validateAndAppendColumnName(translator, queryBuilder,
                                    ((AbstractAttribute) embeddedObject.getAttribute(embeddedColumn.getName()))
                                            .getJPAColumnName(), embeddedColumn.getType());
                        }
                    }

                }
                else
                {
                    validateAndAppendColumnName(translator, queryBuilder, colInfo.getColumnName(), colInfo.getType());
                }

            }
        }
    }

    /**
     * Validate and append column name.
     * 
     * @param translator
     *            the translator
     * @param queryBuilder
     *            the query builder
     * @param b
     *            the b
     * @param clazz
     *            the clazz
     */
    private void validateAndAppendColumnName(CQLTranslator translator, StringBuilder queryBuilder, String b,
            Class<?> clazz)
    {
        String dataType = CassandraValidationClassMapper.getValidationClass(clazz, true);
        translator.appendColumnName(queryBuilder, b, translator.getCQLType(dataType));
        queryBuilder.append(Constants.SPACE_COMMA);
    }

    /**
     * Generates schema for Collection columns.
     * 
     * @param translator
     *            the translator
     * @param collectionColumnInfos
     *            the collection column infos
     * @param queryBuilder
     *            the query builder
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
                    genericTypesBuilder.append(Constants.STR_LT);
                    String keyDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0), true);
                    genericTypesBuilder.append(translator.getCQLType(keyDataType));
                    genericTypesBuilder.append(Constants.SPACE_COMMA);
                    String valueDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(1),
                            true);
                    genericTypesBuilder.append(translator.getCQLType(valueDataType));
                    genericTypesBuilder.append(Constants.STR_GT);
                }
                else if ((ListType.class.getSimpleName().equals(dataType) || SetType.class.getSimpleName().equals(
                        dataType))
                        && genericClasses.size() == 1)
                {
                    genericTypesBuilder.append(Constants.STR_LT);
                    String valueDataType = CassandraValidationClassMapper.getValidationClass(genericClasses.get(0),
                            true);
                    genericTypesBuilder.append(translator.getCQLType(valueDataType));
                    genericTypesBuilder.append(Constants.STR_GT);
                }
                else
                {
                    throw new SchemaGenerationException("Incorrect collection field definition for "
                            + cci.getCollectionColumnName() + ". Generic Types must be defined correctly.");
                }
            }

            if (genericTypesBuilder != null)
            {
                collectionCqlType += genericTypesBuilder.toString();
            }

            translator.appendColumnName(queryBuilder, collectionColumnName, collectionCqlType);
            queryBuilder.append(Constants.SPACE_COMMA);

        }
    }

    /**
     * Creates the inverted index table.
     * 
     * @param tableInfo
     *            the table info
     * @param ksDef
     *            the ks def
     * @throws Exception
     *             the exception
     */
    private void createInvertedIndexTable(TableInfo tableInfo, KsDef ksDef) throws Exception
    {
        CfDef cfDef = getInvertedIndexCF(tableInfo);
        if (cfDef != null)
        {
            try
            {
                cassandra_client.system_add_column_family(cfDef);
            }
            catch (InvalidRequestException irex)
            {
                updateExistingColumnFamily(tableInfo, ksDef, irex);
            }
        }

    }

    /**
     * Gets the inverted index cf.
     * 
     * @param tableInfo
     *            the table info
     * @return the inverted index cf
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     * @throws TException
     *             the t exception
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
     * @throws Exception
     *             the exception
     */
    private void onValidateTables(List<TableInfo> tableInfos, KsDef ksDef) throws Exception
    {
        cassandra_client.set_keyspace(ksDef.getName());
        for (TableInfo tableInfo : tableInfos)
        {
            if (isCql3Enabled(tableInfo) && !tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
            {
                CqlMetadata metadata = new CqlMetadata();
                Map<ByteBuffer, String> name_types = new HashMap<ByteBuffer, String>();
                Map<ByteBuffer, String> value_types = new HashMap<ByteBuffer, String>();
                List<ColumnInfo> columnInfos = tableInfo.getColumnMetadatas();

                List<EmbeddedColumnInfo> compositeColumns = tableInfo.getEmbeddedColumnMetadatas();
                if (compositeColumns != null && !compositeColumns.isEmpty())
                {
                    EmbeddableType embeddableType = compositeColumns.get(0).getEmbeddable();
                    for (ColumnInfo columnInfo : compositeColumns.get(0).getColumns())
                    {
                        name_types
                                .put(ByteBufferUtil.bytes(columnInfo.getColumnName()), UTF8Type.class.getSimpleName());
                        value_types.put(ByteBufferUtil.bytes(columnInfo.getColumnName()),
                                CassandraValidationClassMapper.getValidationClassInstance(columnInfo.getType(), true)
                                        .getName());
                    }

                }
                else
                {
                    name_types.put(ByteBufferUtil.bytes(tableInfo.getIdColumnName()), UTF8Type.class.getSimpleName());
                    value_types.put(ByteBufferUtil.bytes(tableInfo.getIdColumnName()), CassandraValidationClassMapper
                            .getValidationClassInstance(tableInfo.getTableIdType(), true).getName());
                }

                for (ColumnInfo info : columnInfos)
                {
                    name_types.put(ByteBufferUtil.bytes(info.getColumnName()), UTF8Type.class.getSimpleName());
                    value_types.put(ByteBufferUtil.bytes(info.getColumnName()), CassandraValidationClassMapper
                            .getValidationClassInstance(info.getType(), true).getName());
                }

                for (CollectionColumnInfo info : tableInfo.getCollectionColumnMetadatas())
                {
                    name_types
                            .put(ByteBufferUtil.bytes(info.getCollectionColumnName()), UTF8Type.class.getSimpleName());
                    value_types.put(ByteBufferUtil.bytes(info.getCollectionColumnName()),
                            CassandraValidationClassMapper.getValueTypeName(info.getType(), info.getGenericClasses(),
                                    true));
                }

                metadata.setDefault_name_type(UTF8Type.class.getSimpleName());
                metadata.setDefault_value_type(UTF8Type.class.getSimpleName());
                metadata.setName_types(name_types);
                metadata.setValue_types(value_types);
                CQLTranslator translator = new CQLTranslator();
                final String describeTable = "select * from ";
                StringBuilder builder = new StringBuilder(describeTable);
                translator.ensureCase(builder, tableInfo.getTableName(), false);
                builder.append("LIMIT 1");
                cassandra_client.set_cql_version(CassandraConstants.CQL_VERSION_3_0);
                CqlResult cqlResult = cassandra_client.execute_cql3_query(ByteBufferUtil.bytes(builder.toString()),
                        Compression.NONE, ConsistencyLevel.ONE);

                KunderaCoreUtils.printQuery(builder.toString(), showQuery);
                CqlMetadata originalMetadata = cqlResult.getSchema();

                int compareResult = originalMetadata.compareTo(metadata);
                if (compareResult > 0)
                {
                    onLog(tableInfo, metadata, value_types, originalMetadata);
                    throw new SchemaGenerationException(
                            "Schema mismatch!, validation failed. see above table for mismatch");
                }
            }
            else
            {
                onValidateTable(ksDef, tableInfo);
            }
        }
    }

    /**
     * On validate table.
     * 
     * @param ksDef
     *            the ks def
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
     */
    private void onValidateTable(KsDef ksDef, TableInfo tableInfo) throws Exception
    {
        boolean tablefound = false;
        for (CfDef cfDef : ksDef.getCf_defs())
        {
            if (cfDef.getName().equals(tableInfo.getTableName())/*
                                                                 * && (cfDef.
                                                                 * getColumn_type
                                                                 * ().equals(
                                                                 * ColumnFamilyType
                                                                 * .
                                                                 * getInstanceOf
                                                                 * (
                                                                 * tableInfo.getType
                                                                 * ()).name()))
                                                                 */)
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

    /**
     * On validate column.
     * 
     * @param tableInfo
     *            the table info
     * @param cfDef
     *            the cf def
     * @param columnInfo
     *            the column info
     * @throws Exception
     *             the exception
     */
    private void onValidateColumn(TableInfo tableInfo, CfDef cfDef, ColumnInfo columnInfo) throws Exception
    {
        boolean columnfound = false;

        boolean isCounterColumnType = isCounterColumnType(tableInfo, null);

        for (ColumnDef columnDef : cfDef.getColumn_metadata())
        {
            if (isMetadataSame(columnDef, columnInfo, isCql3Enabled(tableInfo), isCounterColumnType))
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
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @param isCounterColumnType
     *            the is counter column type
     * @return true, if is metadata same
     * @throws Exception
     *             the exception
     */
    private boolean isMetadataSame(ColumnDef columnDef, ColumnInfo columnInfo, boolean isCql3Enabled,
            boolean isCounterColumnType) throws Exception
    {
        return isIndexPresent(columnInfo, columnDef, isCql3Enabled, isCounterColumnType);
    }

    /**
     * Update table.
     * 
     * @param ksDef
     *            the ks def
     * @param tableInfo
     *            the table info
     * @throws Exception
     *             the exception
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
                        toUpdate = isCfDefUpdated(columnInfo, cfDef, isCql3Enabled(tableInfo),
                                isCounterColumnType(tableInfo, null), tableInfo) ? true : toUpdate;
                    }
                }
                if (toUpdate)
                {
                    cassandra_client.system_update_column_family(cfDef);
                }
                createIndexUsingThrift(tableInfo, cfDef);
                break;
            }
        }
    }

    /**
     * Checks if is cf def updated.
     * 
     * @param columnInfo
     *            the column info
     * @param cfDef
     *            the cf def
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @param isCounterColumnType
     *            the is counter column type
     * @param tableInfo
     *            the table info
     * @return true, if is cf def updated
     * @throws Exception
     *             the exception
     */
    private boolean isCfDefUpdated(ColumnInfo columnInfo, CfDef cfDef, boolean isCql3Enabled,
            boolean isCounterColumnType, TableInfo tableInfo) throws Exception
    {
        boolean columnPresent = false;
        boolean isUpdated = false;
        for (ColumnDef columnDef : cfDef.getColumn_metadata())
        {
            if (isColumnPresent(columnInfo, columnDef, isCql3Enabled))
            {
                if (!isValidationClassSame(columnInfo, columnDef, isCql3Enabled, isCounterColumnType))
                {
                    columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(
                            columnInfo.getType(), isCql3Enabled));
                    // if (columnInfo.isIndexable() &&
                    // !columnDef.isSetIndex_type())
                    // {
                    // IndexInfo indexInfo =
                    // tableInfo.getColumnToBeIndexed(columnInfo.getColumnName());
                    // columnDef.setIndex_type(CassandraIndexHelper.getIndexType(indexInfo.getIndexType()));
                    // columnDef.isSetIndex_type();
                    // columnDef.setIndex_typeIsSet(true);
                    // columnDef.setIndex_nameIsSet(true);
                    // }
                    // else
                    // {
                    columnDef.setIndex_nameIsSet(false);
                    columnDef.setIndex_typeIsSet(false);
                    // }
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
     * @param columnDef
     *            the column def
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return true, if is indexes present
     * @throws Exception
     *             the exception
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
     * @param columnDef
     *            the column def
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @param isCounterColumnType
     *            the is counter column type
     * @return true, if is indexes present
     * @throws Exception
     *             the exception
     */
    private boolean isValidationClassSame(ColumnInfo columnInfo, ColumnDef columnDef, boolean isCql3Enabled,
            boolean isCounterColumnType) throws Exception
    {
        return (isColumnPresent(columnInfo, columnDef, isCql3Enabled) && columnDef.getValidation_class().endsWith(
                isCounterColumnType ? CounterColumnType.class.getSimpleName() : CassandraValidationClassMapper
                        .getValidationClass(columnInfo.getType(), isCql3Enabled)));
    }

    /**
     * isInedexesPresent method return whether indexes present or not on
     * particular column.
     * 
     * @param columnInfo
     *            the column info
     * @param columnDef
     *            the column def
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @param isCounterColumnType
     *            the is counter column type
     * @return true, if is indexes present
     * @throws Exception
     *             the exception
     */
    private boolean isIndexPresent(ColumnInfo columnInfo, ColumnDef columnDef, boolean isCql3Enabled,
            boolean isCounterColumnType) throws Exception
    {
        return (isValidationClassSame(columnInfo, columnDef, isCql3Enabled, isCounterColumnType) && (columnDef
                .isSetIndex_type() == columnInfo.isIndexable() || (columnDef.isSetIndex_type())));
    }

    /**
     * getColumnMetadata use for getting column metadata for specific
     * columnInfo.
     * 
     * @param columnInfo
     *            the column info
     * @param tableInfo
     *            the table info
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
            // if (!indexInfo.getIndexName().equals(indexInfo.getColumnName()))
            // {
            // columnDef.setIndex_name(indexInfo.getIndexName());
            // }
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
     * Sets the default replication factor.
     * 
     * @param strategy_options
     *            the strategy_options
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
     * @param cfProperties
     *            the c f properties
     * @param builder
     *            the builder
     */
    private void setColumnFamilyProperties(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        if ((cfDef != null && cfProperties != null) || (builder != null && cfProperties != null))
        {
            if (builder != null)
            {
                builder.append(CQLTranslator.WITH_CLAUSE);
            }
            onSetKeyValidation(cfDef, cfProperties, builder);

            onSetCompactionStrategy(cfDef, cfProperties, builder);

            onSetComparatorType(cfDef, cfProperties, builder);

            onSetSubComparator(cfDef, cfProperties, builder);

            // onSetReplicateOnWrite(cfDef, cfProperties, builder);

            onSetCompactionThreshold(cfDef, cfProperties, builder);

            onSetComment(cfDef, cfProperties, builder);

            onSetTableId(cfDef, cfProperties, builder);

            onSetGcGrace(cfDef, cfProperties, builder);

            onSetCaching(cfDef, cfProperties, builder);

            onSetBloomFilter(cfDef, cfProperties, builder);

            onSetRepairChance(cfDef, cfProperties, builder);

            onSetReadRepairChance(cfDef, cfProperties, builder);

            // Strip last AND clause.
            if (builder != null && StringUtils.contains(builder.toString(), CQLTranslator.AND_CLAUSE))
            {
                builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
                // builder.deleteCharAt(builder.length() - 2);
            }

            // Strip last WITH clause.
            if (builder != null && StringUtils.contains(builder.toString(), CQLTranslator.WITH_CLAUSE))
            {
                builder.delete(builder.lastIndexOf(CQLTranslator.WITH_CLAUSE), builder.length());
                // builder.deleteCharAt(builder.length() - 2);
            }
        }
    }

    /**
     * On set read repair chance.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetReadRepairChance(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String dclocalReadRepairChance = cfProperties.getProperty(CassandraConstants.DCLOCAL_READ_REPAIR_CHANCE);
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
                log.error("READ_REPAIR_CHANCE should be double type, Caused by: {}.", nfe);
                throw new SchemaGenerationException(nfe);
            }
        }
    }

    /**
     * On set repair chance.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetRepairChance(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String readRepairChance = cfProperties.getProperty(CassandraConstants.READ_REPAIR_CHANCE);
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

    /**
     * On set bloom filter.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetBloomFilter(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String bloomFilterFpChance = cfProperties.getProperty(CassandraConstants.BLOOM_FILTER_FP_CHANCE);
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

    /**
     * On set caching.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetCaching(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String caching = cfProperties.getProperty(CassandraConstants.CACHING);
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

    /**
     * On set gc grace.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetGcGrace(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String gcGraceSeconds = cfProperties.getProperty(CassandraConstants.GC_GRACE_SECONDS);
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

    /**
     * On set table id.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetTableId(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String id = cfProperties.getProperty(CassandraConstants.ID);
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

    /**
     * On set comment.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetComment(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String comment = cfProperties.getProperty(CassandraConstants.COMMENT);
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

    /**
     * On set replicate on write.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetReplicateOnWrite(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String replicateOnWrite = cfProperties.getProperty(CassandraConstants.REPLICATE_ON_WRITE);
        if (builder != null)
        {
            String replicateOn_Write = CQLTranslator.getKeyword(CassandraConstants.REPLICATE_ON_WRITE);
            builder.append(replicateOn_Write);
            builder.append(CQLTranslator.EQ_CLAUSE);
            builder.append(Boolean.parseBoolean(replicateOnWrite));
            builder.append(CQLTranslator.AND_CLAUSE);
        }
        else if (cfDef != null)
        {
            cfDef.setReplicate_on_write(false);
        }
    }

    /**
     * On set compaction threshold.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetCompactionThreshold(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String maxCompactionThreshold = cfProperties.getProperty(CassandraConstants.MAX_COMPACTION_THRESHOLD);
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
        String minCompactionThreshold = cfProperties.getProperty(CassandraConstants.MIN_COMPACTION_THRESHOLD);
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

    /**
     * On set sub comparator.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetSubComparator(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String subComparatorType = cfProperties.getProperty(CassandraConstants.SUBCOMPARATOR_TYPE);
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

    /**
     * On set comparator type.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetComparatorType(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String comparatorType = cfProperties.getProperty(CassandraConstants.COMPARATOR_TYPE);
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

    /**
     * On set compaction strategy.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetCompactionStrategy(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String compactionStrategy = cfProperties.getProperty(CassandraConstants.COMPACTION_STRATEGY);
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

    /**
     * On set key validation.
     * 
     * @param cfDef
     *            the cf def
     * @param cfProperties
     *            the cf properties
     * @param builder
     *            the builder
     */
    private void onSetKeyValidation(CfDef cfDef, Properties cfProperties, StringBuilder builder)
    {
        String keyValidationClass = cfProperties.getProperty(CassandraConstants.KEY_VALIDATION_CLASS);
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
     * Checks if is cql3 enabled.
     * 
     * @param tableInfo
     *            the table info
     * @return true, if is cql3 enabled
     */
    private boolean isCql3Enabled(TableInfo tableInfo)
    {
        Properties cfProperties = getColumnFamilyProperties(tableInfo);

        String defaultValidationClass = cfProperties != null ? cfProperties
                .getProperty(CassandraConstants.DEFAULT_VALIDATION_CLASS) : null;

        // For normal columns
        boolean isCounterColumnType = isCounterColumnType(tableInfo, defaultValidationClass);
        return containsCompositeKey(tableInfo)
                || containsCollectionColumns(tableInfo)
                || ((cql_version != null && cql_version.equals(CassandraConstants.CQL_VERSION_3_0)) && (containsEmbeddedColumns(tableInfo) || containsElementCollectionColumns(tableInfo)))
                && !isCounterColumnType
                || ((cql_version != null && cql_version.equals(CassandraConstants.CQL_VERSION_3_0)) && !tableInfo
                        .getType().equals(Type.SUPER_COLUMN_FAMILY.name()));
    }

    /**
     * Append property to builder.
     * 
     * @param builder
     *            the builder
     * @param replicateOnWrite
     *            the replicate on write
     * @param keyword
     *            the keyword
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
     * Validate compound key.
     * 
     * @param tableInfo
     *            the table info
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

            Properties cfProperties = getColumnFamilyProperties(tableInfo);
            String defaultValidationClass = null;
            if (tableInfo.getType() != null && tableInfo.getType().equals(Type.SUPER_COLUMN_FAMILY.name()))
            {
                getSuperColumnFamilyMetadata(tableInfo, cfDef, defaultValidationClass);
            }
            else if (tableInfo.getType() != null)
            {
                getColumnFamilyMetadata(tableInfo, cfDef, cfProperties);
            }
            setColumnFamilyProperties(cfDef, cfProperties, null);
            return cfDef;
        }

        /**
         * Gets the super column family metadata.
         * 
         * @param tableInfo
         *            the table info
         * @param cfDef
         *            the cf def
         * @param defaultValidationClass
         *            the default validation class
         * @return the super column family metadata
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
         * Gets the column family metadata.
         * 
         * @param tableInfo
         *            the table info
         * @param cfDef
         *            the cf def
         * @param cfProperties
         *            the cf properties
         * @return the column family metadata
         */
        private void getColumnFamilyMetadata(TableInfo tableInfo, CfDef cfDef, Properties cfProperties)
        {
            String defaultValidationClass = cfProperties != null ? cfProperties
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
                            // if
                            // (!indexInfo.getIndexName().equals(indexInfo.getColumnName()))
                            // {
                            // columnDef.setIndex_name(indexInfo.getIndexName());
                            // }
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
         * Gets the counter column family metadata.
         * 
         * @param tableInfo
         *            the table info
         * @param cfDef
         *            the cf def
         * @return the counter column family metadata
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
                        // if
                        // (!indexInfo.getIndexName().equals(indexInfo.getColumnName()))
                        // {
                        // columnDef.setIndex_name(indexInfo.getIndexName());
                        // }
                    }
                    columnDef.setName(columnInfo.getColumnName().getBytes());
                    columnDef.setValidation_class(CounterColumnType.class.getName());
                    counterColumnDefs.add(columnDef);
                }
            }
            cfDef.setColumn_metadata(counterColumnDefs);
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
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
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
                if (relation != null)
                {
                    EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            relation.getTargetEntity());
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
            }
            return isValid;
        }

        /**
         * validate embedded column .
         * 
         * @param metadata
         *            the metadata
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
         *            the metadata
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

    /**
     * Print log in case schema doesn't match!.
     * 
     * @param tableInfo
     *            the table info
     * @param metadata
     *            the metadata
     * @param value_types
     *            the value_types
     * @param originalMetadata
     *            the original metadata
     * @throws CharacterCodingException
     *             the character coding exception
     */
    private void onLog(TableInfo tableInfo, CqlMetadata metadata, Map<ByteBuffer, String> value_types,
            CqlMetadata originalMetadata) throws CharacterCodingException
    {
        System.out.format("Persisted Schema for " + tableInfo.getTableName());
        System.out.format("\n");

        System.out.format("Column Name: \t\t  Column name type");
        System.out.format("\n");

        printInfo(originalMetadata);

        System.out.format("\n");
        System.out.format("Mapped schema for " + tableInfo.getTableName());

        System.out.format("\n");
        System.out.format("Column Name: \t\t  Column name type");
        System.out.format("\n");
        printInfo(metadata);
    }

    /**
     * TODO:: need to use Message formatter for formatting message.
     * 
     * @param metadata
     *            CQL metadata
     * @throws CharacterCodingException
     *             the character coding exception
     */
    private void printInfo(CqlMetadata metadata) throws CharacterCodingException
    {

        Iterator<ByteBuffer> nameIter = metadata.getName_types().keySet().iterator();
        Iterator<ByteBuffer> valueIter = metadata.getValue_types().keySet().iterator();

        while (nameIter.hasNext())
        {
            ByteBuffer key = nameIter.next();
            System.out.format(ByteBufferUtil.string(key) + " \t\t " + metadata.getName_types().get(key));
            System.out.format("\n");
        }

        System.out.format("Column Name: \t\t  Column Value type");
        System.out.format("\n");
        while (valueIter.hasNext())
        {
            ByteBuffer key = valueIter.next();
            System.out.format(ByteBufferUtil.string(key) + " \t\t " + metadata.getValue_types().get(key));
            System.out.format("\n");
        }
        System.out.format("\n");
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