/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.query;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * A factory for creating SingleColumnFilter objects.
 */
public class SingleColumnFilterFactory
{

    /** The Constant EQUAL. */
    public static final SingleColumnFilterFactory EQUAL = new SingleColumnFilterFactory(CompareOp.EQUAL,
            BinaryComparatorFactory.INSTANCE);

    /** The Constant NOT_EQUAL. */
    public static final SingleColumnFilterFactory NOT_EQUAL = new SingleColumnFilterFactory(CompareOp.NOT_EQUAL,
            BinaryComparatorFactory.INSTANCE);

    /** The Constant GREATER. */
    public static final SingleColumnFilterFactory GREATER = new SingleColumnFilterFactory(CompareOp.GREATER,
            BinaryComparatorFactory.INSTANCE);

    /** The Constant LESS. */
    public static final SingleColumnFilterFactory LESS = new SingleColumnFilterFactory(CompareOp.LESS,
            BinaryComparatorFactory.INSTANCE);

    /** The Constant GREATER_OR_EQUAL. */
    public static final SingleColumnFilterFactory GREATER_OR_EQUAL = new SingleColumnFilterFactory(
            CompareOp.GREATER_OR_EQUAL, BinaryComparatorFactory.INSTANCE);

    /** The Constant LESS_OR_EQUAL. */
    public static final SingleColumnFilterFactory LESS_OR_EQUAL = new SingleColumnFilterFactory(
            CompareOp.LESS_OR_EQUAL, BinaryComparatorFactory.INSTANCE);

    /** The Constant LIKE. */
    public static final SingleColumnFilterFactory LIKE = new SingleColumnFilterFactory(CompareOp.EQUAL,
            LikeComparatorFactory.INSTANCE);

    /** The Constant REGEXP. */
    public static final SingleColumnFilterFactory REGEXP = new SingleColumnFilterFactory(CompareOp.EQUAL,
            RegexComparatorFactory.INSTANCE);

    /** The operator. */
    private final CompareOp operator;

    /**
     * Gets the operator.
     * 
     * @return the operator
     */
    public CompareOp getOperator()
    {
        return operator;
    }

    /** The comparator factory. */
    private final Function<byte[], ByteArrayComparable> comparatorFactory;

    /**
     * Instantiates a new single column filter factory.
     * 
     * @param operator
     *            the operator
     * @param comparatorFactory
     *            the comparator factory
     */
    public SingleColumnFilterFactory(CompareOp operator, Function<byte[], ByteArrayComparable> comparatorFactory)
    {
        this.operator = operator;
        this.comparatorFactory = comparatorFactory;
    }

    /**
     * Creates the.
     * 
     * @param family
     *            the family
     * @param column
     *            the column
     * @param value
     *            the value
     * @return the filter
     */
    public Filter create(byte[] family, byte[] column, byte[] value)
    {
        return new SingleColumnValueFilter(family, column, operator, comparatorFactory.apply(value));
    }
}
