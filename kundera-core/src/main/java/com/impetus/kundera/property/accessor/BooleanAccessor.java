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
public class BooleanAccessor implements PropertyAccessor<Boolean>
{

    @Override
    public Boolean fromBytes(byte[] data) throws PropertyAccessException
    {
        return (data == null || data.length == 0) ? false : data[0] != 0x00;
    }

    @Override
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        if (object != null)
        {

            Boolean b = (Boolean) object;

            return new byte[] { (byte) (b ? 0x01 : 0x00) }; // bool -> {1 byte}
        }
        return null;
    }

    @Override
    public String toString(Object object)
    {
        return object.toString();
    }

    @Override
    public Boolean fromString(String s) throws PropertyAccessException
    {
        try
        {
            Boolean b = new Boolean(s);
            return b;
        }
        catch (NumberFormatException e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

}
