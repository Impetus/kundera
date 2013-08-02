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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

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
        Calendar cal = Calendar.getInstance();
        Date d = new Date();
        if (b == null)
        {
            return null;
        }
        LongAccessor longAccessor = new LongAccessor();
        d.setTime(longAccessor.fromBytes(targetClass, b));
        cal.setTime(d);
        return cal;
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
        Calendar cal = (Calendar) object;
        // return
        // DateAccessor.getFormattedObect(cal.getTime().toString()).getBytes();
        LongAccessor longAccessor = new LongAccessor();
        return longAccessor.toBytes(cal.getTime().getTime());
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
        Calendar calendar = (Calendar) object;

        if (calendar == null)
        {
            return null;
        }
        return String.valueOf(calendar.getTime().getTime());
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
        if (s == null)
        {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        Date d;
        // d = (Date)DATE_FORMATTER.parse(s);
        d = DateAccessor.getDateByPattern(s);
        cal.setTime(d);
        return cal;
    }

    @Override
    public Calendar getCopy(Object object)
    {
        if (object == null)
            return null;
        Calendar c = (Calendar) object;
        Calendar copy = Calendar.getInstance();
        copy.setTimeInMillis(c.getTimeInMillis());
        return copy;
    }

    public Calendar getInstance(Class<?> clazz)
    {
        return Calendar.getInstance();
    }
}
