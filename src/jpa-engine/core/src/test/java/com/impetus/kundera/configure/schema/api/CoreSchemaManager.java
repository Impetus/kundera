/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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

import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author vivek.mishra
 */
public class CoreSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    private static List<TableInfo> tables;

    private static String currentAction;

    public CoreSchemaManager(String clientFactory, Map<String, Object> externalProperties, final KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
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
        this.tables = tableInfos;
        currentAction = "update";
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
        this.tables = tableInfos;
        currentAction = "create";
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
        this.tables = tableInfos;
        currentAction = "create-drop";
    }

    @Override
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {

            this.tables = null;
            currentAction = "drop";
        }
    }

    @Override
    public boolean validateEntity(Class clazz)
    {
        return true;
    }

    static boolean validateAction(String action)
    {
        if ((action.equals("create-drop") || action.equals("update") || action.equals("create"))
                && action.equals(currentAction))
        {
            return tables != null;
        }
        else
        {
            return action.equals(currentAction) && tables == null;
        }

    }

}
