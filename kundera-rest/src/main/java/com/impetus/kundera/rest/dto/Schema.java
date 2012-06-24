/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.rest.dto;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */

@XmlRootElement
public class Schema
{
    private String schemaName;
    
    private List<Table> tables;

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @return the tables
     */
    public List<Table> getTables()
    {
        return tables;
    }

    /**
     * @param tables the tables to set
     */
    public void setTables(List<Table> tables)
    {
        this.tables = tables;
    }  
    
    public void addTable(Table table) {
        if(tables == null) {
            tables = new ArrayList<Table>();
        }
        tables.add(table);
    } 

}
