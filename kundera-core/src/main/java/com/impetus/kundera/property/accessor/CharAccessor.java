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
 * @author Amresh Singh
 * 
 */
public class CharAccessor implements PropertyAccessor<Character>
{

    @Override
    public Character fromBytes(byte[] data) throws PropertyAccessException
    {
        if (data == null || data.length != 2)
            return 0x0;

        return (char) ((0xff & data[0]) << 8 | (0xff & data[1]) << 0);
    }

    @Override
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        Character data = (Character) object;
        return new byte[] { (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff), };
    }

    @Override
    public String toString(Object object)
    {
        return object.toString();
    }

    @Override
    public Character fromString(String s) throws PropertyAccessException
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
            throw new PropertyAccessException(e.getMessage());
        }
    }

}
