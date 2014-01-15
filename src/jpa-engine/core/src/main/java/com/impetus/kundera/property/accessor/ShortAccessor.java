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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class ShortAccessor.
 * 
 * @author Amresh Singh
 */
public class ShortAccessor implements PropertyAccessor<Short>
{
    public static Logger log = LoggerFactory.getLogger(ShortAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Short fromBytes(Class targetClass, byte[] data)
    {
        if (data == null || data.length != 2)
        {
            if (log.isWarnEnabled())
            {
                log.warn("Bytes length not equal to 2");
            }
            return 0x0;
        }
        return (short) ((0xff & data[0]) << 8 | (0xff & data[1]) << 0);

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
        Short s = null;
        if (object != null)
        {

            if (object.getClass().isAssignableFrom(String.class))
            {
                s = Short.valueOf(object.toString());
            }
            else
            {
                s = (Short) object;
            }
            return new byte[] { (byte) ((s >> 8) & 0xff), (byte) ((s >> 0) & 0xff), };
        }
        return null;
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
        return object != null ? object.toString() : null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Short fromString(Class targetClass, String s)
    {
        try
        {

            if (s == null)
            {
                return null;
            }
            Short sh = new Short(s);
            return sh;
        }
        catch (NumberFormatException e)
        {
            log.error("Number format exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Short getCopy(Object object)
    {
        return object != null ? (Short) object : null;
    }

    public Short getInstance(Class<?> clazz)
    {
        return Short.MAX_VALUE;
    }
}
