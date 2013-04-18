/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Interprets JPA query and holds conditions to pass on to client
 * @author amresh.singh
 */
public class OracleNoSQLQueryInterpreter
{
    private boolean isById;
    
    //Select columns
    private String[] selectColumns;
    
    //Represents clauses
    private Map<String, Object> filterConditions;
    
    String operator;   //Inter-clause operator
    
    /**
     * Default constructor
     */
    public OracleNoSQLQueryInterpreter(String[] selectColumns)
    {
        this.selectColumns = selectColumns;
    }

    /**
     * @return the isById
     */
    public boolean isById()
    {
        return isById;
    }

    /**
     * @param isById the isById to set
     */
    public void setById(boolean isById)
    {
        this.isById = isById;
    }

    /**
     * @return the selectColumns
     */
    public String[] getSelectColumns()
    {
        return selectColumns;
    }

    

    /**
     * @param selectColumns the selectColumns to set
     */
    public void setSelectColumns(String[] selectColumns)
    {
        this.selectColumns = selectColumns;
    }

    /**
     * @return the filterCondition
     */
    public Map<String, Object> getFilterConditions()
    {
        return filterConditions;
    }    
    

    /**
     * @return the operator
     */
    public String getOperator()
    {
        return operator;
    }

    /**
     * @param operator the operator to set
     */
    public void setOperator(String operator)
    {
        this.operator = operator;
    }

    /**
     * Adds filter condition
     * @param columnName
     * @param columnValue
     */
    public void addFilterCondition(String columnName, Object columnValue)
    {
        if(filterConditions == null)
        {
            filterConditions = new HashMap<String, Object>();
        }
        filterConditions.put(columnName, columnValue);
    }  

}
