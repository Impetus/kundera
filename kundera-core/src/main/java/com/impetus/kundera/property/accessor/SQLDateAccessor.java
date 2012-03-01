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
import java.sql.Date;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class SQLDateAccessor.
 *
 * @author amresh.singh
 */
public class SQLDateAccessor implements PropertyAccessor<Date>
{

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Date fromBytes(byte[] b) throws PropertyAccessException
    {
        String s;
        try
        {
            s = new String(b, Constants.ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new PropertyAccessException(e.getMessage());
        }

        return fromString(s);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    @Override
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        Date d = (Date) object;
        return d.toString().getBytes();
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object)
     */
    @Override
    public String toString(Object object)
    {
        return object.toString();
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String)
     */
    @Override
    public Date fromString(String s) throws PropertyAccessException
    {
        Date d = Date.valueOf(s);
        return d;
    }

}
