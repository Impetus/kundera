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
 * The Class BooleanAccessor.
 * 
 * @author Amresh Singh
 */
public class BooleanAccessor implements PropertyAccessor<Boolean>
{

    private final static Logger log = LoggerFactory.getLogger(BooleanAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Boolean fromBytes(Class targetClass, byte[] data)
    {
        return (data == null || data.length == 0) ? false : data[0] != 0x00;
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
        if (object != null)
        {
            Boolean b = (Boolean) object;

            return new byte[] { (byte) (b ? 0x01 : 0x00) }; // bool -> {1 byte}
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
    public Boolean fromString(Class targetClass, String s)
    {
        try
        {
            if (s == null)
            {
                return null;
            }

            Boolean b = new Boolean(s);
            return b;
        }
        catch (NumberFormatException e)
        {
            log.error("Number format exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Boolean getCopy(Object object)
    {
        return object != null ? new Boolean((Boolean) object) : null;
    }

    public Boolean getInstance(Class<?> clazz)
    {
        return Boolean.TRUE;
    }
}
