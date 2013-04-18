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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
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
    public BigDecimal fromBytes(Class targetClass, byte[] b)
    {
        String s;
        try
        {
            if (b == null)
            {
                return null;
            }
            s = new String(b, Constants.ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new PropertyAccessException(e);
        }
        return fromString(targetClass, s);
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
        return b.toString().getBytes();
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
        return object != null ? b: null;
    }

    public BigDecimal getInstance(Class<?> clazz)
    {
        return BigDecimal.TEN;
    }
}
