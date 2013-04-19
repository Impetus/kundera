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

import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Interprets JPA query and holds conditions to pass on to client
 * @author amresh.singh
 */
public class OracleNoSQLQueryInterpreter
{ 
    
    //Select columns
    private String[] selectColumns;
    
    
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
    

    

}
