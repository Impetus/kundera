/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.cache;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.query.Person;
import com.impetus.kundera.query.Person.Day;

/**
 * @author vivek.mishra
 * junit for {@link NonOperationalCache}.
 * Current implementation doesn't do anything. So junit needs to modified later.
 *
 */
public class NonOperationalCacheTest
{

    @Test
    public void test()
    {
        NonOperationalCache noOpCache = new NonOperationalCache();
        Person person = new Person();
        person.setAge(32);
        person.setDay(Day.SATURDAY);
        person.setPersonId("p1");
        person.setPersonName("Milan Kundera");
        
        noOpCache.put("p1", person);

        // Non operational cache doesn't do anything. So such assertions won't work! 
        // 
//        Person cachedPerson = (Person) noOpCache.get("p1");
//        Assert.assertEquals(person, cachedPerson);

        noOpCache.evict(Person.class);
        noOpCache.evictAll();
        noOpCache.evict(Person.class, person);
        Assert.assertFalse(noOpCache.contains(Person.class, person));
        Assert.assertNull(noOpCache.get("p1"));
    }

}
