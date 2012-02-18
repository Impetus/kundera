/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

import java.util.Date;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessException;

/**
 * The Class DateAccessorTest.
 * 
 * @author vivek.mishra
 */
public class DateAccessorTest
{

    /** The accessor. */
    private DateAccessor accessor;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new DateAccessor();
    }

    /**
     * Test date from string.
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    @Test
    public void testDateFromString() throws PropertyAccessException
    {
        String dateInMMddYYHHmmss = "02/01/2012 00:00:00";
        String newDateAsStr = "Wed Feb 01 07:58:02 IST 2012";
        String dateInMMddYY = "02/01/2012";
        String dateInMMddYYDash = "02-01-2012";
        String dateInMMMddYYYY = "Feb/02/2012";
        Assert.assertNotNull(accessor.fromString(Date.class, dateInMMddYYDash));
        Assert.assertNotNull(accessor.fromString(Date.class, dateInMMddYYHHmmss));
        Assert.assertNotNull(accessor.fromString(Date.class, newDateAsStr));
        Assert.assertNotNull(accessor.fromString(Date.class, dateInMMMddYYYY));
        Assert.assertNotNull(accessor.fromString(Date.class, dateInMMddYY));
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
