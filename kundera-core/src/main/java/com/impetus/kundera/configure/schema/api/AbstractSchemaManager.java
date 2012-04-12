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
import java.util.Set;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.metadata.MetadataUtils;
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
    protected String host;

    /** The kundera_client variable. */
    protected String kundera_client_factory;

    /** The database name variable. */
    protected String databaseName;

    /** The table infos variable . */
    protected List<TableInfo> tableInfos;

    /** The operation variable. */
    protected String operation;

    /** The use secondry index variable. */
    protected boolean useSecondryIndex = false;

    /**
     * Initialise with configured client factory.
     * 
     * @param clientFactory  specific client factory.
     */
    protected AbstractSchemaManager(String clientFactory)
    {
        kundera_client_factory = clientFactory;
    }

    // @Override
    /**
     * Export schema handles the handleOperation method.
     * 
     * @param hbase
     */
    protected void exportSchema()
    {

        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        // Actually, start 1 pu.
        Map<String, List<TableInfo>> puToSchemaCol = appMetadata.getSchemaMetadata().getPuToSchemaMetadata();
        Set<String> pus = puToSchemaCol.keySet();
        for (String pu : pus)
        {
            // Get persistence unit metadata
            puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(pu);
            if (kundera_client_factory != null
                    && kundera_client_factory.equalsIgnoreCase(puMetadata.getProperties().getProperty(
                            PersistenceProperties.KUNDERA_CLIENT_FACTORY)))
            {
                port = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_PORT);
                host = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_NODES);
                databaseName = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
                useSecondryIndex = MetadataUtils.useSecondryIndex(pu);
                // get type of schema of operation.
                operation = puMetadata.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);

                // invoke handle operation.
                if (operation != null && initiateClient())
                {
                    tableInfos = puToSchemaCol.get(pu);
                    handleOperations(tableInfos);
                }
            }
        }
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
