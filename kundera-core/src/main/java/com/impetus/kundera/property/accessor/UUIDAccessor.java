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

import java.util.UUID;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class UUIDAccessor.
 * 
 * @author kcarlson
 */
public class UUIDAccessor implements PropertyAccessor<UUID>
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromBytes(java.lang.Class,
     * byte[])
     */
    @Override
    public UUID fromBytes(Class targetClass, byte[] bytes) throws PropertyAccessException
    {
        try
        {
            if(bytes == null)
            {
                return null;
            }
            return UUID.nameUUIDFromBytes(bytes);
        }
        catch (Exception e)
        {
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
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        try
        {
            if(object == null)
            {
                return null;
            }
            UUID uuid = (UUID) object;
            return asByteArray(uuid);
        }
        catch (Exception e)
        {
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
        if(object == null)
        {
            return null;
        }
        return ((UUID) object).toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.Class,
     * java.lang.String)
     */
    @Override
    public UUID fromString(Class targetClass, String s) throws PropertyAccessException
    {
        try
        {
            if(s == null)
            {
                return null;
            }
            return UUID.fromString(s);
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e);
        }
    }

    /**
     * Returns as byte array.
     * 
     * @param uuid
     *            UUID
     * @return byte[] array.
     */
    private static byte[] asByteArray(UUID uuid)
    {
        if(uuid == null)
        {
            return null;
        }
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0; i < 8; i++)
        {
            buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++)
        {
            buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return buffer;
    }

    public UUID getInstance(Class<?> clazz)
    {
        return UUID.randomUUID();
    }
}