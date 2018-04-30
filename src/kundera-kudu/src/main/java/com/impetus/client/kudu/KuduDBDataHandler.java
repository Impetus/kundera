/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu;

import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class KuduDBDataHandler.
 * 
 * @author karthikp.manchala
 */
public class KuduDBDataHandler
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KuduDBDataHandler.class);

    /** The kundera metadata. */
    private KunderaMetadata kunderaMetadata;

    /**
     * Instantiates a new kudu db data handler.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public KuduDBDataHandler(KunderaMetadata kunderaMetadata)
    {
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * Adds the to row.
     * 
     * @param row
     *            the row
     * @param jpaColumnName
     *            the jpa column name
     * @param value
     *            the value
     * @param type
     *            the type
     */
    public static void addToRow(PartialRow row, String jpaColumnName, Object value, Type type)
    {
        if (value == null)
        {
            row.setNull(jpaColumnName);
        }
        else
        {
            switch (type)
            {
            case BINARY:
                row.addBinary(jpaColumnName, (byte[]) value);
                break;
            case BOOL:
                row.addBoolean(jpaColumnName, (Boolean) value);
                break;
            case DOUBLE:
                row.addDouble(jpaColumnName, (Double) value);
                break;
            case FLOAT:
                row.addFloat(jpaColumnName, (Float) value);
                break;
            case INT16:
                row.addShort(jpaColumnName, (Short) value);
                break;
            case INT32:
                row.addInt(jpaColumnName, (Integer) value);
                break;
            case INT64:
                row.addLong(jpaColumnName, (Long) value);
                break;
            case INT8:
                row.addByte(jpaColumnName, (Byte) value);
                break;
            case STRING:
                row.addString(jpaColumnName, (String) value);
                break;
            case UNIXTIME_MICROS:
            default:
                logger.error(type + " type is not supported by Kudu");
                throw new KunderaException(type + " type is not supported by Kudu");
            }
        }
    }

    /**
     * Gets the predicate.
     * 
     * @param column
     *            the column
     * @param operator
     *            the operator
     * @param type
     *            the type
     * @param key
     *            the key
     * @return the predicate
     */
    public static KuduPredicate getPredicate(ColumnSchema column, KuduPredicate.ComparisonOp operator, Type type,
            Object key)
    {
        switch (type)
        {
        case BINARY:
            return KuduPredicate.newComparisonPredicate(column, operator, (byte[]) key);
        case BOOL:
            return KuduPredicate.newComparisonPredicate(column, operator, (Boolean) key);
        case DOUBLE:
            return KuduPredicate.newComparisonPredicate(column, operator, (Double) key);
        case FLOAT:
            return KuduPredicate.newComparisonPredicate(column, operator, (Float) key);
        case INT16:
            return KuduPredicate.newComparisonPredicate(column, operator, (Short) key);
        case INT32:
            return KuduPredicate.newComparisonPredicate(column, operator, (Integer) key);
        case INT64:
            return KuduPredicate.newComparisonPredicate(column, operator, (Long) key);
        case INT8:
            return KuduPredicate.newComparisonPredicate(column, operator, (Byte) key);
        case STRING:
            return KuduPredicate.newComparisonPredicate(column, operator, (String) key);
        case UNIXTIME_MICROS:
        default:
            logger.error(type + " type is not supported by Kudu");
            throw new KunderaException(type + " type is not supported by Kudu");
        }
    }

    /**
     * Gets the equal comparison predicate.
     * 
     * @param column
     *            the column
     * @param type
     *            the type
     * @param key
     *            the key
     * @return the equal comparison predicate
     */
    public static KuduPredicate getEqualComparisonPredicate(ColumnSchema column, Type type, Object key)
    {
        return getPredicate(column, KuduPredicate.ComparisonOp.EQUAL, type, key);

    }

    /**
     * Gets the column value.
     * 
     * @param result
     *            the result
     * @param jpaColumnName
     *            the jpa column name
     * @return the column value
     */
    public static Object getColumnValue(RowResult result, String jpaColumnName)
    {

        if (result.isNull(jpaColumnName))
        {
            return null;
        }

        switch (result.getColumnType(jpaColumnName))
        {
        case BINARY:
            return result.getBinary(jpaColumnName);
        case BOOL:
            return result.getBoolean(jpaColumnName);
        case DOUBLE:
            return result.getDouble(jpaColumnName);
        case FLOAT:
            return result.getFloat(jpaColumnName);
        case INT16:
            return result.getShort(jpaColumnName);
        case INT32:
            return result.getInt(jpaColumnName);
        case INT64:
            return result.getLong(jpaColumnName);
        case INT8:
            return result.getByte(jpaColumnName);
        case STRING:
            return result.getString(jpaColumnName);
        case UNIXTIME_MICROS:
        default:
            logger.error(jpaColumnName + " type is not supported by Kudu");
            throw new KunderaException(jpaColumnName + " type is not supported by Kudu");

        }
    }

    /**
     * Parses the.
     * 
     * @param type
     *            the type
     * @param value
     *            the value
     * @return the object
     */
    public static Object parse(Type type, String value)
    {
        value = value.replaceAll("^['\\\"]|['\\\"]$", "");
        switch (type)
        {
        case BINARY:
            return value.getBytes();
        case BOOL:
            return Boolean.parseBoolean(value);
        case DOUBLE:
            return Double.parseDouble(value);
        case FLOAT:
            return Float.parseFloat(value);
        case INT16:
            return Short.parseShort(value);
        case INT32:
            return Integer.parseInt(value);
        case INT64:
            return Long.parseLong(value);
        case INT8:
            return Byte.parseByte(value);
        case STRING:
            return value;
        case UNIXTIME_MICROS:
        default:
            logger.error(type + " type is not supported by Kudu");
            throw new KunderaException(type + " type is not supported by Kudu");

        }
    }

    /**
     * Checks for column.
     * 
     * @param schema
     *            the schema
     * @param columnName
     *            the column name
     * @return true, if successful
     */
    public static boolean hasColumn(Schema schema, String columnName)
    {
        try
        {
            schema.getColumn(columnName);
            return true;
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }
    }

    public static KuduPredicate getInPredicate(ColumnSchema column, List<Object> values)
    {

        return KuduPredicate.newInListPredicate(column, values);
    }

}
