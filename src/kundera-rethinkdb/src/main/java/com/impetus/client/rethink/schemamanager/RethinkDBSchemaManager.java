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

import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class RethinkDBSchemaManager.
 * 
 * @author karthikp.manchala
 */
public class RethinkDBSchemaManager extends AbstractSchemaManager implements SchemaManager {

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
        KunderaMetadata kunderaMetadata) {
        super(clientFactory, externalProperties, kunderaMetadata);
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema() {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#validate(java.util.List)
     */
    @Override
    protected void validate(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#update(java.util.List)
     */
    @Override
    protected void update(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create(java.util.List)
     */
    @Override
    protected void create(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create_drop(java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager# exportSchema(java.lang.String,
     * java.util.List)
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas) {
        super.exportSchema(persistenceUnit, schemas);
    }

}
