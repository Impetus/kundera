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

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import com.google.common.base.Function;

/**
 * A factory for creating RegexComparator objects.
 * 
 * @author karthikp.manchala
 */
public class RegexComparatorFactory implements Function<byte[], ByteArrayComparable>
{

    /** The Constant INSTANCE. */
    public static final RegexComparatorFactory INSTANCE = new RegexComparatorFactory();


    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    public ByteArrayComparable apply(byte[] bytes)
    {
        return new RegexStringComparator(new String(bytes));
    }
}
