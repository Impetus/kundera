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

import java.sql.Timestamp;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class SQLTimestampAccessor.
 * 
 * @author amresh.singh
 */
public class SQLTimestampAccessor implements PropertyAccessor<Timestamp>
{
    public static Logger log = LoggerFactory.getLogger(SQLTimestampAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Timestamp fromBytes(Class targetClass, byte[] b)
    {

        // String s;
        // try
        // {
        // if(b == null)
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

            return new Timestamp(longAccessor.fromBytes(targetClass, b));
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
        // if(object == null)
        // {
        // return null;
        // }
        // Timestamp t = (Timestamp) object;
        // return t.toString().getBytes();
        try
        {
            if (object == null)
            {
                return null;
            }
            LongAccessor longAccessor = new LongAccessor();
            return longAccessor.toBytes(((Timestamp) object).getTime());
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
        Timestamp timeStamp = (Timestamp) object;

        if (timeStamp == null)
        {
            return null;
        }

        return String.valueOf(timeStamp.getTime());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Timestamp fromString(Class targetClass, String s)
    {
        if (s == null)
        {
            return null;
        }
        if (StringUtils.isNumeric(s))
        {
            return new Timestamp(Long.parseLong(s));
        }
        Timestamp t = Timestamp.valueOf(s);
        return t;
    }

    @Override
    public Timestamp getCopy(Object object)
    {
        Timestamp ts = (Timestamp) object;
        return ts != null ? new Timestamp(ts.getTime()) : null;
    }

    public Timestamp getInstance(Class<?> clazz)
    {
        return new Timestamp(Integer.MAX_VALUE);
    }
}
