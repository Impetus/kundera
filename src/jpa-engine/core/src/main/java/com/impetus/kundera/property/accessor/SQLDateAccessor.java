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

import java.sql.Date;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class SQLDateAccessor.
 * 
 * @author amresh.singh
 */
public class SQLDateAccessor implements PropertyAccessor<Date>

{
    public static Logger log = LoggerFactory.getLogger(SQLDateAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Date fromBytes(Class targetClass, byte[] b)
    {
        try
        {
            if (b == null)
            {
                return null;
            }

            // In case date.getTime() is stored in DB.
            LongAccessor longAccessor = new LongAccessor();

            return new Date(longAccessor.fromBytes(targetClass, b));
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

        try
        {
            if (object == null)
            {
                return null;
            }
            LongAccessor longAccessor = new LongAccessor();
            return longAccessor.toBytes(((Date) object).getTime());
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
        Date date = (Date) object;

        if (date == null)
        {
            return null;
        }

        return String.valueOf(date.getTime())/*date.toString()*/;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Date fromString(Class targetClass, String s)
    {

        if (s == null)
        {
            return null;
        }
        if (StringUtils.isNumeric(s))
        {
            return new Date(Long.parseLong(s));
        }
        Date d = Date.valueOf(s);
        return d;
    }

    @Override
    public Date getCopy(Object object)
    {
        Date d = (Date) object;
        return d != null ? new Date(d.getTime()) : null;
    }

    public Date getInstance(Class<?> clazz)
    {
        return new Date(Integer.MAX_VALUE);
    }
}
