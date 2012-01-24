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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 *
 */
public class CalendarAccessor implements PropertyAccessor<Calendar>
{

    /** The Constant DATE_FORMATTER. */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("dd-MM-yy HH:mm:ss", Locale.ENGLISH);    
    
    @Override
    public Calendar fromBytes(byte[] b) throws PropertyAccessException
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


    @Override
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        Calendar cal = (Calendar) object;
        return DATE_FORMATTER.format(cal.getTime()).getBytes();
    }


    @Override
    public String toString(Object object)
    {

        return object.toString();
    }


    @Override
    public Calendar fromString(String s) throws PropertyAccessException
    {
        Calendar cal = Calendar.getInstance();
        Date d;
        try
        {
            d = (Date)DATE_FORMATTER.parse(s);
        }
        catch (ParseException e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
        cal.setTime(d);
        return cal;
    }
    
}
