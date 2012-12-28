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

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.property.PropertyAccessorHelper;

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

    private Map<String, Double> min;

    private Map<String, Double> max;

    private Clause clause;

    private Object value;

    private String fieldName;

    private Map<String, Object> fields;

    private static Map<String,Clause> clauseMapper = new HashMap<String,Clause>();
    static
    {
        clauseMapper.put("AND", Clause.INTERSECT);
        clauseMapper.put("OR", Clause.UNION);
    }
    enum Clause
    {
        UNION, INTERSECT;
    }

    static Clause getMappedClause(String intraClause)
    {
        return clauseMapper.get(intraClause);
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
        if (this.fields != null)
        {
            fields.put(fieldName, value);
        }
    }

    Map<String, Double> getMin()
    {
        return min;
    }

    void setMin(String field, Object fieldValue)
    {
        this.min = new HashMap<String, Double>(1);
        this.min.put(field, !StringUtils.isNumeric(fieldValue.toString()) ? Double.valueOf(PropertyAccessorHelper.getString(fieldValue).hashCode()): Double.valueOf(fieldValue.toString()));
    }

    Map<String, Double> getMax()
    {
        return max;
    }

    void setMax(String field, Object fieldValue)
    {
        this.max = new HashMap<String, Double>(1);
        this.max.put(field, !StringUtils.isNumeric(fieldValue.toString()) ? Double.valueOf(PropertyAccessorHelper.getString(fieldValue).hashCode()): Double.valueOf(fieldValue.toString()));
    }

    String getFieldName()
    {
        return fieldName;
    }

    void setFieldName(String fieldName)
    {
        this.fieldName = fieldName;
        if (fields == null)
        {
            fields = new HashMap<String, Object>(2);
        }
        fields.put(fieldName, null);
    }

    Map<String, Object> getFields()
    {
        return fields;
    }
}
