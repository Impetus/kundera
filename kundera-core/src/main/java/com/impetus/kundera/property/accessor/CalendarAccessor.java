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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class CalendarAccessor.
 * 
 * @author amresh.singh
 */
public class CalendarAccessor implements PropertyAccessor<Calendar>
{

    /** The Constant DATE_FORMATTER. */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public Calendar fromBytes(Class targetClass, byte[] b)
    {
        String s;
        try
        {
            String s1 = new String(b);
            s = new String(b, Constants.ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new PropertyAccessException(e.getMessage());
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
        Calendar cal = (Calendar) object;
        return DateAccessor.getFormattedObect(cal.getTime().toString()).getBytes();
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
    public Calendar fromString(Class targetClass, String s)
    {
        Calendar cal = Calendar.getInstance();
        Date d;
        // d = (Date)DATE_FORMATTER.parse(s);
        d = DateAccessor.getDateByPattern(s);
        cal.setTime(d);
        return cal;
    }

}
