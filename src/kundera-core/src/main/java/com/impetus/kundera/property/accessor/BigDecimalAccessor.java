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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class BigDecimalAccessor.
 * 
 * @author amresh.singh
 */
public class BigDecimalAccessor implements PropertyAccessor<BigDecimal>
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public BigDecimal fromBytes(Class targetClass, byte[] bytes)
    {
        if (bytes == null)
        {
            return null;
        }
        int scale = (((bytes[0]) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | ((bytes[3] & 0xff)));

        byte[] bibytes = Arrays.copyOfRange(bytes, 4, bytes.length);

        BigInteger bi = new BigInteger(bibytes);

        return new BigDecimal(bi, scale);
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

        BigDecimal b = (BigDecimal) object;
        final int scale = b.scale();
        final BigInteger unscaled = b.unscaledValue();
        final byte[] value = unscaled.toByteArray();
        final byte[] bytes = new byte[value.length + 4];
        bytes[0] = (byte) (scale >>> 24);
        bytes[1] = (byte) (scale >>> 16);
        bytes[2] = (byte) (scale >>> 8);
        bytes[3] = (byte) (scale >>> 0);
        System.arraycopy(value, 0, bytes, 4, value.length);
        return bytes;
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
    public BigDecimal fromString(Class targetClass, String s)
    {

        return s != null ? new BigDecimal(s) : null;
    }

    @Override
    public BigDecimal getCopy(Object object)
    {
        BigDecimal b = (BigDecimal) object;
        return object != null ? b : null;
    }

    public BigDecimal getInstance(Class<?> clazz)
    {
        return BigDecimal.TEN;
    }
}
