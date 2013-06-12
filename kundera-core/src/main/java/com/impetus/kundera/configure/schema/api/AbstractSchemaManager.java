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
package com.impetus.kundera.configure.schema.api;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Abstract Schema Manager has abstract method to handle
 * {@code SchemaOperationType}.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public abstract class AbstractSchemaManager
{

    /** The pu metadata variable. */
    protected PersistenceUnitMetadata puMetadata;

    /** The port variable. */
    protected String port;

    /** The host variable . */
    protected String[] hosts;

    /** The kundera_client variable. */
    protected String clientFactory;

    /** The database name variable. */
    protected String databaseName;

    /** The table infos variable . */
    protected List<TableInfo> tableInfos;

    /** The operation variable. */
    protected String operation;

    protected List<Schema> schemas = null;

    protected Connection conn = null;

    protected DataStore dataStore = null;

    protected Map<String, Object> externalProperties;

    /**
     * Initialise with configured client factory.
     * 
     * @param clientFactory
     *            specific client factory.
     * @param externalProperties
     */
    protected AbstractSchemaManager(String clientFactory, Map<String, Object> externalProperties)
    {
        this.clientFactory = clientFactory;
        this.externalProperties = externalProperties;
    }

//     @Override
    /**
     * Export schema handles the handleOperation method.
     * 
     * @param hbase
     */
    protected void exportSchema(final String persistenceUnit,List<TableInfo> tables)
    {

//        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        // Actually, start 1 pu.
//        Map<String, List<TableInfo>> puToSchemaCol = appMetadata.getSchemaMetadata().getPuToSchemaMetadata();
//        Set<String> pus = puToSchemaCol.keySet();
//        for (String pu : pus)
//        {
            // Get persistence unit metadata
            PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
            String paramString = externalProperties != null ? (String) externalProperties
                    .get(PersistenceProperties.KUNDERA_CLIENT_FACTORY) : null;
            if (clientFactory != null
                    && ((clientFactory.equalsIgnoreCase(puMetadata.getProperties().getProperty(
                            PersistenceProperties.KUNDERA_CLIENT_FACTORY))) || (paramString != null && clientFactory
                            .equalsIgnoreCase(paramString))))
            {
                readConfigProperties(puMetadata);

                // invoke handle operation.
                if (operation != null && initiateClient())
                {
                    tableInfos = tables;
                    handleOperations(tables);
                }
            }
     //   }
    }

    /**
     * @param pu
     */
    private void readConfigProperties(final PersistenceUnitMetadata puMetadata)
    {
        String hostName = null;
        String portName = null;
        String operationType = null;
        String schemaName = null;
        if (externalProperties != null)
        {
            portName = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            hostName = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            schemaName = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            // get type of schema of operation.
            operationType = (String) externalProperties.get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        }
        if (portName == null)
            portName = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_PORT);
        if (hostName == null)
            hostName = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_NODES);
        if (schemaName == null)
            schemaName = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
        // get type of schema of operation.
        if (operationType == null)
            operationType = puMetadata.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);

        String[] hostArray = hostName.split(",");
        hosts = new String[hostArray.length];
        for (int i = 0; i < hostArray.length; i++)
        {
            hosts[i] = hostArray[i].trim();
        }

        this.port = portName;
        this.databaseName = schemaName;
        this.operation = operationType;
        // this.useSecondryIndex = isSecondryIndexEnabled;
    }

    /**
     * Initiate client to initialize with client specific schema.
     * 
     * @return true, if successful
     */
    protected abstract boolean initiateClient();

    /**
     * Validates the schema.
     * 
     * @param tableInfos
     *            the table infos
     */
    protected abstract void validate(List<TableInfo> tableInfos);

    /**
     * Update.
     * 
     * @param tableInfos
     *            the table infos
     */
    protected abstract void update(List<TableInfo> tableInfos);

    /**
     * Creates the.
     * 
     * @param tableInfos
     *            the table infos
     */
    protected abstract void create(List<TableInfo> tableInfos);

    /**
     * Create_drop.
     * 
     * @param tableInfos
     *            the table infos
     */
    protected abstract void create_drop(List<TableInfo> tableInfos);

    /**
     * handleOperations method handles the all operation on the basis of
     * operationType
     */

    /**
     * enum class for operation type
     */
    /**
     * The Enum ScheamOperationType.
     */
    public enum ScheamOperationType
    {

        /** The createdrop. */
        createdrop,
        /** The create. */
        create,
        /** The validate. */
        validate,
        /** The update. */
        update;

        /**
         * Gets the single instance of ScheamOperationType.
         * 
         * @param operation
         *            the operation
         * @return single instance of ScheamOperationType
         */
        private static ScheamOperationType getInstance(String operation)
        {
            if (operation.equalsIgnoreCase("create-drop"))
            {
                return ScheamOperationType.createdrop;
            }
            return ScheamOperationType.valueOf(ScheamOperationType.class, operation);
        }
    }

    /**
     * Handle operations.
     * 
     * @param tableInfos
     *            the table infos
     */
    private void handleOperations(List<TableInfo> tableInfos)
    {
        ScheamOperationType operationType = ScheamOperationType.getInstance(operation);

        switch (operationType)
        {
        case createdrop:
            create_drop(tableInfos);
            break;
        case create:
            create(tableInfos);
            break;
        case update:
            update(tableInfos);
            break;
        case validate:
            validate(tableInfos);
            break;
        }
    }
}
