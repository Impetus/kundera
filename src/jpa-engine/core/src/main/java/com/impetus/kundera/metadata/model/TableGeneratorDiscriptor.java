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
package com.impetus.kundera.metadata.model;

import javax.persistence.TableGenerator;
import javax.persistence.UniqueConstraint;

/**
 * TableGeneratorDiscriptor class holds the information about table generator.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class TableGeneratorDiscriptor
{
    private static final String default_table_name = "kundera_sequences";

    private static final String default_pkColumn_name = "sequence_name";

    private static final String default_valueColumn_name = "sequence_value";

    private static final int default_allocation_size = 50;

    private static final int default_initial_value = 1;

    private String table;

    private String catalog;

    private String schema;

    private String pkColumnName;

    private String valueColumnName;

    private String pkColumnValue;

    private int initialValue;

    private int allocationSize;

    private UniqueConstraint[] uniqueConstraints;

    public TableGeneratorDiscriptor(TableGenerator tableGenerator, String defaultSchemaName, String defaultPkColumnValue)
    {
        this.table = tableGenerator.table().isEmpty() ? default_table_name : tableGenerator.table();
        this.schema = tableGenerator.schema().isEmpty() ? defaultSchemaName : tableGenerator.schema();
        this.pkColumnName = tableGenerator.pkColumnName().isEmpty() ? default_pkColumn_name : tableGenerator
                .pkColumnName();
        this.valueColumnName = tableGenerator.valueColumnName().isEmpty() ? default_valueColumn_name : tableGenerator
                .valueColumnName();
        this.pkColumnValue = tableGenerator.pkColumnValue().isEmpty() ? defaultPkColumnValue : tableGenerator
                .pkColumnValue();
        this.initialValue = tableGenerator.initialValue() != 0 ? tableGenerator.initialValue() : default_initial_value;
        this.allocationSize = tableGenerator.allocationSize();
    }

    public TableGeneratorDiscriptor(String defaultSchemaName, String defaultPkColumnValue)
    {
        this.table = default_table_name;
        this.schema = defaultSchemaName;
        this.pkColumnName = default_pkColumn_name;
        this.valueColumnName = default_valueColumn_name;
        this.pkColumnValue = defaultPkColumnValue;
        this.initialValue = default_initial_value;
        this.allocationSize = default_allocation_size;
    }

    /**
     * @return the table
     */
    public String getTable()
    {
        return table;
    }

    /**
     * @return the catalog
     */
    public String getCatalog()
    {
        return catalog;
    }

    /**
     * @return the schema
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     * @return the pkColumnName
     */
    public String getPkColumnName()
    {
        return pkColumnName;
    }

    /**
     * @return the valueColumnName
     */
    public String getValueColumnName()
    {
        return valueColumnName;
    }

    /**
     * @return the pkColumnValue
     */
    public String getPkColumnValue()
    {
        return pkColumnValue;
    }

    /**
     * @return the initialValue
     */
    public int getInitialValue()
    {
        return initialValue;
    }

    /**
     * @return the allocationSize
     */
    public int getAllocationSize()
    {
        return allocationSize;
    }

    /**
     * @return the uniqueConstraints
     */
    public UniqueConstraint[] getUniqueConstraints()
    {
        return uniqueConstraints;
    }

}
