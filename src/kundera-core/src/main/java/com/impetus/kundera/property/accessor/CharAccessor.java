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
 * The Class CharAccessor.
 * 
 * @author Amresh Singh
 */
public class CharAccessor implements PropertyAccessor<Character>
{

    private final static Logger log = LoggerFactory.getLogger(CharAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Character fromBytes(Class targetClass, byte[] data)
    {
        if (data == null || data.length != 2)
        {
            if (log.isWarnEnabled())
            {
                log.warn("Data length is not matching");
            }
            return 0x0;
        }
        return (char) ((0xff & data[0]) << 8 | (0xff & data[1]) << 0);
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
        if (object == null)
        {
            return null;
        }
        Character data = null;
        if (object.getClass().isAssignableFrom(String.class))
        {
            data = ((String) object).charAt(0);
        }
        else
        {
            data = (Character) object;
        }

        return new byte[] { (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff), };
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
        if (object == null)
        {
            return null;
        }

        return object.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Character fromString(Class targetClass, String s)
    {
        try
        {
            if (s == null)
            {
                log.error("Can't convert String " + s + " to character");
                throw new PropertyAccessException("Can't convert String " + s + " to character");
            }

            Character c = null;
            if (s.length() == 1)
            {
                c = s.charAt(0);
            }
            else
            {
                c = Character.MIN_VALUE;
            }

            return c;
        }
        catch (NumberFormatException e)
        {
            log.error("Number format exception caught,Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Character getCopy(Object object)
    {
        return object != null ? new Character((Character) object) : null;
    }

    public Character getInstance(Class<?> clazz)
    {
        return Character.MAX_VALUE;
    }
}
