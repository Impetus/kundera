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

package com.impetus.kundera.configure.schema;

import com.impetus.kundera.KunderaException;

/**
 * Exception class for all type of exceptions thrown by SchemaManager during
 * generating schema.
 * 
 * @author Kuldeep.Kumar
 */
public class SchemaGenerationException extends KunderaException
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3855497974944993364L;

    /** The data store name. */
    private String dataStoreName;

    /** The schema name. */
    private String schemaName;

    /** The table name. */
    private String tableName;

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param dataStore
     *            the data store
     * @param schema
     *            the schema
     */
    public SchemaGenerationException(String arg0, String dataStore, String schema)
    {
        super(arg0);
        this.dataStoreName = dataStore;
        this.schemaName = schema;
    }

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param arg0
     *            the arg0
     * @param dataStore
     *            the data store
     * @param schema
     *            the schema
     * @param table
     *            the table
     */
    public SchemaGenerationException(String arg0, String dataStore, String schema, String table)
    {
        super(arg0);
        this.dataStoreName = dataStore;
        this.schemaName = schema;
        this.tableName = table;
    }

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param arg0
     *            the arg0
     */
    public SchemaGenerationException(Throwable arg0)
    {
        super(arg0);
    }

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param arg0
     *            the arg0
     * @param dataStore
     *            the data store
     */
    public SchemaGenerationException(Throwable arg0, String dataStore)
    {
        super(arg0);
        this.dataStoreName = dataStore;
    }

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param arg0
     *            the arg0
     * @param dataStore
     *            the data store
     * @param schema
     *            the schema
     */
    public SchemaGenerationException(Throwable arg0, String dataStore, String schema)
    {
        super(arg0);
        this.dataStoreName = dataStore;
        this.schemaName = schema;
    }

    /**
     * Instantiates a new schemaGeneration exception.
     * 
     * @param arg0
     *            the arg0
     * @param arg1
     *            the arg1
     * @param dataStore
     *            the data store
     */
    public SchemaGenerationException(String arg0, Throwable arg1, String dataStore)
    {
        super(arg0, arg1);
        this.dataStoreName = dataStore;
    }

    public SchemaGenerationException(String arg0, Throwable arg1, String dataStoreName, String databaseName)
    {
        super(arg0, arg1);
        this.dataStoreName = dataStoreName;
        this.schemaName = databaseName;
    }

    public SchemaGenerationException(String arg0)
    {
        super(arg0);
    }
}
