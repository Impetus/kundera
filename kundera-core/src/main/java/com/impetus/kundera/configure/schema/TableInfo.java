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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * TableInfo class holds table creation related information.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class TableInfo
{
    /** The table name. */
    private String tableName;

    /** The column metadatas. */
    private List<ColumnInfo> columnMetadatas;

    /** The table id type. */
    private String tableIdType;

    /** The type. */
    private String type;

    /** The embedded column metadatas. */
    private List<EmbeddedColumnInfo> embeddedColumnMetadatas;

    /** The is indexable. */
    private boolean isIndexable;

    /**
     * Instantiates a new table info.
     */
    public TableInfo(String tableName, boolean isIndexable, String tableSchemaType, String idClassType)
    {
        this.tableName = tableName;
        this.isIndexable = isIndexable;
        this.type = tableSchemaType;
        this.tableIdType = idClassType;
    }
    

    /**
     * Equals method compare two object of TableInfo on the basis of their
     * name,type and tableIdType .
     * 
     * @param Object
     *            instance.
     * 
     * @return boolean value.
     */
    @Override
    public boolean equals(Object obj)
    {

        return obj != null && obj instanceof TableInfo && ((TableInfo) obj).tableName != null ? this.tableName != null
                && this.tableName.equals(((TableInfo) obj).tableName)
                && this.tableIdType.equals(((TableInfo) obj).tableIdType) : false;

    }

    @Override 
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    
    @Override
    public String toString()
    {
        StringBuilder strBuilder = new StringBuilder("tableIdType:==> ");
        strBuilder.append(tableIdType);
        strBuilder.append(" | tableName: ==>");
        strBuilder.append(tableName);
        strBuilder.append(" | type: ==>");
        strBuilder.append(type);
        strBuilder.append("isIndexable: ==>");
        strBuilder.append(isIndexable);
        return strBuilder.toString();
    }
    /**
     * Gets the table name.
     * 
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Sets the table name.
     * 
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Gets the table id type.
     * 
     * @return the tableIdType
     */
    public String getTableIdType()
    {
        return tableIdType;
    }

    /**
     * Sets the table id type.
     * 
     * @param tableIdType
     *            the tableIdType to set
     */
    public void setTableIdType(String tableIdType)
    {
        this.tableIdType = tableIdType;
    }

    /**
     * Gets the column metadatas.
     * 
     * @return the columnMetadatas
     */
    public List<ColumnInfo> getColumnMetadatas()
    {

        if(this.columnMetadatas == null)
        {
            this.columnMetadatas = new ArrayList<ColumnInfo>();
        }
        
        return columnMetadatas;
    }

    public void addColumnInfo(ColumnInfo columnInfo)
    {
        if(this.columnMetadatas == null)
        {
            this.columnMetadatas = new ArrayList<ColumnInfo>();
        }
        columnMetadatas.add(columnInfo);
    }
    
    public void addEmbeddedColumnInfo(EmbeddedColumnInfo embdColumnInfo)
    {
        if(this.embeddedColumnMetadatas == null)
        {
            this.embeddedColumnMetadatas = new ArrayList<EmbeddedColumnInfo>();
        }
        embeddedColumnMetadatas.add(embdColumnInfo);
    }
    /**
     * Gets the embedded column metadatas.
     * 
     * @return the embeddedColumnMetadatas
     */
    public List<EmbeddedColumnInfo> getEmbeddedColumnMetadatas()
    {
        return embeddedColumnMetadatas;
    }


    /**
     * Gets the type.
     * 
     * @return the type
     */
    public String getType()
    {
        return type;
    }

    /**
     * Checks if is indexable.
     * 
     * @return the isIndexable
     */
    public boolean isIndexable()
    {
        return isIndexable;
    }

    /**
     * Sets the indexable.
     * 
     * @param isIndexable
     *            the isIndexable to set
     */
    public void setIndexable(boolean isIndexable)
    {
        this.isIndexable = isIndexable;
    }

    /**
     * Sets the type.
     * 
     * @param type
     *            the type to set
     */
    public void setType(String type)
    {
        this.type = type;
    }
}
