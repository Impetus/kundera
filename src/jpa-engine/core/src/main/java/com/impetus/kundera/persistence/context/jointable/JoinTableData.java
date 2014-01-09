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
package com.impetus.kundera.persistence.context.jointable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds data required prior to persisting records in Join Table
 * 
 * @author amresh.singh
 */
public class JoinTableData
{
    private String joinTableName;

    private String schemaName;

    private Class<?> entityClass;

    private String joinColumnName;

    private String inverseJoinColumnName;

    private boolean isProcessed;

    public static enum OPERATION
    {
        INSERT, UPDATE, DELETE
    }

    /**
     * Operation to be performed on this Join Table data
     * 
     * @See {@link OPERATION}
     */
    private OPERATION operation;

    /**
     * Key -> Primary key of entity at the Join column side Value -> Set of
     * primary keys of entities at the inverse join column side
     */
    Map<Object, Set<Object>> joinTableRecords;

    public JoinTableData(OPERATION operation, String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Class<?> entityClass)
    {
        this.operation = operation;
        this.schemaName = schemaName;
        this.joinTableName = joinTableName;
        this.joinColumnName = joinColumnName;
        this.inverseJoinColumnName = inverseJoinColumnName;
        this.entityClass = entityClass;

        joinTableRecords = new HashMap<Object, Set<Object>>();
    }

    /**
     * @return the joinTableName
     */
    public String getJoinTableName()
    {
        return joinTableName;
    }

    /**
     * @param joinTableName
     *            the joinTableName to set
     */
    public void setJoinTableName(String joinTableName)
    {
        this.joinTableName = joinTableName;
    }

    /**
     * @return the joinColumnName
     */
    public String getJoinColumnName()
    {
        return joinColumnName;
    }

    /**
     * @param joinColumnName
     *            the joinColumnName to set
     */
    public void setJoinColumnName(String joinColumnName)
    {
        this.joinColumnName = joinColumnName;
    }

    /**
     * @return the inverseJoinColumnName
     */
    public String getInverseJoinColumnName()
    {
        return inverseJoinColumnName;
    }

    /**
     * @param inverseJoinColumnName
     *            the inverseJoinColumnName to set
     */
    public void setInverseJoinColumnName(String inverseJoinColumnName)
    {
        this.inverseJoinColumnName = inverseJoinColumnName;
    }

    /**
     * @return the joinTableRecords
     */
    public Map<Object, Set<Object>> getJoinTableRecords()
    {
        return joinTableRecords;
    }

    /**
     * @return the entityClass
     */
    public Class<?> getEntityClass()
    {
        return entityClass;
    }

    /**
     * @param entityClass
     *            the entityClass to set
     */
    public void setEntityClass(Class<?> entityClass)
    {
        this.entityClass = entityClass;
    }

    /**
     * @return the operation
     */
    public OPERATION getOperation()
    {
        return operation;
    }

    /**
     * @param operation
     *            the operation to set
     */
    public void setOperation(OPERATION operation)
    {
        this.operation = operation;
    }

    /**
     * @param joinTableRecords
     *            the joinTableRecords to set
     */
    public void addJoinTableRecord(Object key, Set<Object> values)
    {
        Set<Object> existingValues = joinTableRecords.get(key);
        if (existingValues == null)
        {
            existingValues = new HashSet<Object>();
            existingValues.addAll(values);
            joinTableRecords.put(key, existingValues);
        }
        else
        {
            existingValues.addAll(values);
        }
    }

    /**
     * @return the isProcessed
     */
    public boolean isProcessed()
    {
        return isProcessed;
    }

    /**
     * @param isProcessed
     *            the isProcessed to set
     */
    public void setProcessed(boolean isProcessed)
    {
        this.isProcessed = isProcessed;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param schemaName
     *            the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

}
