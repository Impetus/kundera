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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;

/**
 * @author amresh.singh
 * 
 */
public class EnumAccessorTest
{

    enum Day
    {
        MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
    }

    Day day1 = Day.MONDAY;

    Day day2 = Day.TUESDAY;

    EnumAccessor accessor = new EnumAccessor();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromBytes(byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        try
        {
            byte[] b = accessor.toBytes(day1);
            String s = new String(b, Constants.ENCODING);
            Assert.assertEquals(day1.MONDAY.toString(), s);

        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (UnsupportedEncodingException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        String s = accessor.toString(day1);
        System.out.println(s);
        Assert.assertEquals(Day.MONDAY.toString(), s);

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromString(java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {

    }

}
