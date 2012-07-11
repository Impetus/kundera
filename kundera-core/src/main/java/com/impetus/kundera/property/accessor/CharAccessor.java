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

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class CharAccessor.
 * 
 * @author Amresh Singh
 */
public class CharAccessor implements PropertyAccessor<Character>
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Character fromBytes(Class targetClass, byte[] data)
    {
        if (data == null || data.length != 2)
            return 0x0;

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
        Character data = (Character) object;
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
            if (s == null || s.length() != 1)
            {
                throw new PropertyAccessException("Can't convert String " + s + " to character");
            }

            Character c = s.charAt(0);
            return c;
        }
        catch (NumberFormatException e)
        {
            throw new PropertyAccessException(e);
        }
    }
    public Character getInstance(Class<?> clazz)
    {
        return Character.MAX_VALUE ;
    }
}
