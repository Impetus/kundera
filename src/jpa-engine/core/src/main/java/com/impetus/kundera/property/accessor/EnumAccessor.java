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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 * 
 */
@SuppressWarnings("rawtypes")
public class EnumAccessor implements PropertyAccessor<Enum>
{

    private final static Logger log = LoggerFactory.getLogger(EnumAccessor.class);

    @Override
    public Enum fromBytes(Class targetClass, byte[] b)
    {
        String s = null;
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
            log.error("Unsupported encoding exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
        return fromString(targetClass, s);
    }

    @Override
    public byte[] toBytes(Object object)
    {
        if (object == null)
        {
            return null;
        }
        String s = toString(object);
        try
        {
            return s.getBytes(Constants.ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Unsupported encoding exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public String toString(Object object)
    {
        if (object == null)
        {
            return null;
        }
        Enum en = (Enum) object;
        return en.name();
    }

    @Override
    public Enum fromString(Class targetClass, String string)
    {
        if (targetClass != null && string != null)
        {
            try
            {
                return Enum.valueOf(targetClass, string.trim());
            }
            catch (IllegalArgumentException ex)
            {
                log.error("Illegal argument exception, Caused by {}.", ex);
                throw new PropertyAccessException(ex);
            }
        }

        return null;
    }

    @Override
    public Enum getCopy(Object object)
    {
        if (object != null)
        {
            return fromString(object.getClass(), toString(object));
        }
        return null;
    }

    public Enum getInstance(Class<?> clazz)
    {
        return null;
    }
}
