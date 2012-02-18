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
 * The Class FloatAccessor.
 * 
 * @author Amresh Singh
 */
public class FloatAccessor implements PropertyAccessor<Float>
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Float fromBytes(Class targetClass, byte[] data) throws PropertyAccessException
    {
        if (data == null || data.length != 4)
            return (float) 0x0;

        return Float.intBitsToFloat(toInt(data));
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
        return fromInt(Float.floatToRawIntBits((Float) object));
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

    /**
     * From int.
     * 
     * @param data
     *            the data
     * @return the byte[]
     */
    private byte[] fromInt(int data)
    {
        return new byte[] { (byte) ((data >> 24) & 0xff), (byte) ((data >> 16) & 0xff), (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff), };
    }

    /**
     * To int.
     * 
     * @param data
     *            the data
     * @return the int
     */
    private int toInt(byte[] data)
    {
        if (data == null || data.length != 4)
            return 0x0;

        return (int) ( // NOTE: type cast not necessary for int
        (0xff & data[0]) << 24 | (0xff & data[1]) << 16 | (0xff & data[2]) << 8 | (0xff & data[3]) << 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Float fromString(Class targetClass, String s) throws PropertyAccessException
    {
        try
        {
            Float f = new Float(s);
            return f;
        }
        catch (NumberFormatException e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

}
