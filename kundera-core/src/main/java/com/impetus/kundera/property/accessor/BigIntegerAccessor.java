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

import java.math.BigInteger;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class BigIntegerAccessor.
 * 
 * @author amresh.singh
 */
public class BigIntegerAccessor implements PropertyAccessor<BigInteger>
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public BigInteger fromBytes(Class targetClass, byte[] b)
    {
        return b != null ? new BigInteger(b) : null;
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
        BigInteger b = (BigInteger) object;
        return b.toByteArray();
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
    public BigInteger fromString(Class targetClass, String s)
    {
        return s != null ? new BigInteger(s) : null;
    }

    @Override
    public BigInteger getCopy(Object object)
    {
        BigInteger i = (BigInteger) object;
        return object != null ? new BigInteger(i.toByteArray()) : null;
    }

    public BigInteger getInstance(Class<?> clazz)
    {
        return BigInteger.TEN;
    }
}