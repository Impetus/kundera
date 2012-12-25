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
package com.impetus.client.redis;

import java.util.HashMap;
import java.util.Map;

/**
 * Query interpreter responsible for: <li>Determine if it is a query by id.</li>
 * <li>holds value and clause(UNION or INTERSECT REDIS clause for sorted set)</li>
 * 
 * @author vivek.mishra
 * 
 */
class RedisQueryInterpreter
{

    private boolean isById;

    private Map<String, Object> min;

    private Map<String, Object> max;

    private Clause clause;

    private Object value;

    private String fieldName;

    enum Clause
    {
        UNION, INTERSECT;
    }

    /**
     * Default constructor
     */
    public RedisQueryInterpreter()
    {
    }

    boolean isById()
    {
        return isById;
    }

    void setById(boolean isById)
    {
        this.isById = isById;
    }

    Clause getClause()
    {
        return clause;
    }

    void setClause(Clause clause)
    {
        this.clause = clause;
    }

    boolean isByRange()
    {
        return min != null || max != null;
    }

    Object getValue()
    {
        return value;
    }

    void setValue(Object value)
    {
        this.value = value;
    }

    Map<String, Object> getMin()
    {
        return min;
    }

    void setMin(String field, Object fieldValue)
    {
        this.min = new HashMap<String, Object>(1);
        this.min.put(field, fieldValue);
    }

    Map<String, Object> getMax()
    {
        return max;
    }

    void setMax(String field, Object fieldValue)
    {
        this.max = new HashMap<String, Object>(1);
        this.max.put(field, fieldValue);
    }

    String getFieldName()
    {
        return fieldName;
    }

    void setFieldName(String fieldName)
    {
        this.fieldName = fieldName;
    }
}
