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
package com.impetus.kundera.property.accessor;

import java.sql.Time;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class SQLTimeAccessor.
 * 
 * @author amresh.singh
 */
public class SQLTimeAccessor implements PropertyAccessor<Time>
{
    public static Logger log = LoggerFactory.getLogger(SQLTimeAccessor.class);
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Time fromBytes(Class targetClass, byte[] b)
    {
        // String s;
        // try
        // {
        //
        // if (b == null)
        // {
        // return null;
        // }
        // s = new String(b, Constants.ENCODING);
        // }
        // catch (UnsupportedEncodingException e)
        // {
        // throw new PropertyAccessException(e);
        // }
        // return fromString(targetClass, s);
        try
        {
            if (b == null)
            {
                return null;
            }

            // In case date.getTime() is stored in DB.
            LongAccessor longAccessor = new LongAccessor();

            return new Time(longAccessor.fromBytes(targetClass, b));
        }
        catch (Exception e)
        {
            log.error("Error occured, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    @Override
    public byte[] toBytes(Object object)
    {

        // if (object == null)
        // {
        // return null;
        // }
        // Time t = (Time) object;
        // return t.toString().getBytes();

        try
        {
            if (object == null)
            {
                return null;
            }
            LongAccessor longAccessor = new LongAccessor();
            return longAccessor.toBytes(((Time) object).getTime());
            // return DATE_FORMATTER.format(((Date)
            // date)).getBytes(Constants.ENCODING);
        }
        catch (Exception e)
        {
            log.error("Error occured, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object)
     */
    @Override
    public String toString(Object object)
    {
        Time time = (Time) object;

        if (time == null)
        {
            return null;
        }

        return String.valueOf(time.getTime());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Time fromString(Class targetClass, String s)
    {
        if (s == null)
        {
            return null;
        }

        if (StringUtils.isNumeric(s))
        {
            return new Time(Long.parseLong(s));
        }
        Time t = Time.valueOf(s);
        return t;
    }

    @Override
    public Time getCopy(Object object)
    {
        Time t = (Time) object;
        return t != null ? new Time(t.getTime()) : null;
    }

    public Time getInstance(Class<?> clazz)
    {
        return new Time(Integer.MAX_VALUE);
    }
}
