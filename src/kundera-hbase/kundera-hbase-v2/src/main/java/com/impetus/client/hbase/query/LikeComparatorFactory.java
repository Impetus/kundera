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

import static java.lang.Character.isLetterOrDigit;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

/**
 * A factory for creating LikeComparator objects.
 */
public class LikeComparatorFactory implements Function<byte[], ByteArrayComparable>
{

    /** The Constant INSTANCE. */
    public static final LikeComparatorFactory INSTANCE = new LikeComparatorFactory();

    /**
     * Like to regex.
     * 
     * @param like
     *            the like
     * @return the string
     */
    static String likeToRegex(String like)
    {
        StringBuilder builder = new StringBuilder();
        boolean wasPercent = false;
        for (int i = 0; i < like.length(); ++i)
        {
            char c = like.charAt(i);
            if (isPlain(c))
            {
                if (wasPercent)
                {
                    wasPercent = false;
                    builder.append(".*");
                }
                builder.append(c);
            }
            else if (wasPercent)
            {
                wasPercent = false;
                if (c == '%')
                {
                    builder.append('%');
                }
                else
                {
                    builder.append(".*").append(c);
                }
            }
            else
            {
                if (c == '%')
                {
                    wasPercent = true;
                }
                else
                {
                    builder.append('\\').append(c);
                }
            }
        }
        if (wasPercent)
        {
            builder.append(".*");
        }
        return builder.toString();
    }

    /**
     * Checks if is plain.
     * 
     * @param c
     *            the c
     * @return true, if is plain
     */
    private static boolean isPlain(char c)
    {
        return isLetterOrDigit(c) || "_@ ~`\"'#<>,;;=!&".indexOf(c) >= 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    public ByteArrayComparable apply(byte[] bytes)
    {
        return new RegexStringComparator(likeToRegex(new String(bytes)));
    }
}
