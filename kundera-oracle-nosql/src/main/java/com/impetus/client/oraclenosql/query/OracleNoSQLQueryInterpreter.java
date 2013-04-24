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

import java.util.Queue;

/**
 * Interprets JPA query and holds conditions to pass on to client
 * @author amresh.singh
 */
public class OracleNoSQLQueryInterpreter
{ 
    
    //Select columns
    private String[] selectColumns;
    
    private boolean isFindById;   
    private Object rowKey;
    private String operatorWithRowKey;
    
    private Queue clauseQueue;   

    
    /**
     * Default constructor
     */
    public OracleNoSQLQueryInterpreter(String[] selectColumns)
    {
        this.selectColumns = selectColumns;
    }


    /**
     * @return the clauseQueue
     */
    public Queue getClauseQueue()
    {
        return clauseQueue;
    }


    /**
     * @param clauseQueue the clauseQueue to set
     */
    public void setClauseQueue(Queue clauseQueue)
    {
        this.clauseQueue = clauseQueue;
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
     * @return the isFindById
     */
    public boolean isFindById()
    {
        return isFindById;
    }


    /**
     * @param isFindById the isFindById to set
     */
    public void setFindById(boolean isFindById)
    {
        this.isFindById = isFindById;
    }


    /**
     * @return the rowKey
     */
    public Object getRowKey()
    {
        return rowKey;
    }


    /**
     * @param rowKey the rowKey to set
     */
    public void setRowKey(Object rowKey)
    {
        this.rowKey = rowKey;
    }


    /**
     * @return the operatorWithRowKey
     */
    public String getOperatorWithRowKey()
    {
        return operatorWithRowKey;
    }


    /**
     * @param operatorWithRowKey the operatorWithRowKey to set
     */
    public void setOperatorWithRowKey(String operatorWithRowKey)
    {
        this.operatorWithRowKey = operatorWithRowKey;
    }        
    
}
