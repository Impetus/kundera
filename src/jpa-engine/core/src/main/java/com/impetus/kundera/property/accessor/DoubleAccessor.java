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
 * The Class DoubleAccessor.
 * 
 * @author Amresh Singh
 */
public class DoubleAccessor implements PropertyAccessor<Double>
{

    private final static Logger log = LoggerFactory.getLogger(DoubleAccessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Double fromBytes(Class targetClass, byte[] data)
    {
        if (data == null || data.length != 8)
            return (double) 0x0;

        return Double.longBitsToDouble(toLong(data));
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
        return object != null ? fromLong(Double.doubleToRawLongBits((Double) object)) : null;
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

    /**
     * To long.
     * 
     * @param data
     *            the data
     * @return the long
     */
    private long toLong(byte[] data)
    {
        if (data == null || data.length != 8)
            return 0x0;
        // ----------
        return (long) (
        // (Below) convert to longs before shift because digits
        // are lost with ints beyond the 32-bit limit
        (long) (0xff & data[0]) << 56 | (long) (0xff & data[1]) << 48 | (long) (0xff & data[2]) << 40
                | (long) (0xff & data[3]) << 32 | (long) (0xff & data[4]) << 24 | (long) (0xff & data[5]) << 16
                | (long) (0xff & data[6]) << 8 | (long) (0xff & data[7]) << 0);
    }

    /**
     * From long.
     * 
     * @param data
     *            the data
     * @return the byte[]
     */
    private byte[] fromLong(long data)
    {
        return new byte[] { (byte) ((data >> 56) & 0xff), (byte) ((data >> 48) & 0xff), (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff), (byte) ((data >> 24) & 0xff), (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff), };
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Double fromString(Class targetClass, String s)
    {
        try
        {
            if (s == null)
            {
                return null;
            }
            Double d = new Double(s);
            return d;
        }
        catch (NumberFormatException e)
        {
            log.error("Number format exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Double getCopy(Object object)
    {
        return object != null ? new Double((Double) object) : null;
    }

    public Double getInstance(Class<?> clazz)
    {
        return Double.MAX_VALUE;
    }
}
