/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.utils;

import java.lang.reflect.Field;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * junit for {@link ReflectUtils}
 *
 */
public class ReflectUtilsTest
{

    @Test
    public void test() throws NoSuchFieldException, SecurityException
    {
        try
        {
            Class clazz = ReflectUtils.classForName("Person", this.getClass().getClassLoader());
            
        }catch(KunderaException cfex)
        {
            Assert.assertNotNull(cfex.getMessage());
        }
        
        Class clazz = ReflectUtils.classForName("com.impetus.kundera.query.Person", this.getClass().getClassLoader());
        Assert.assertNotNull(clazz);
        Assert.assertEquals(Person.class, clazz);
        
        
        Field field = Person.class.getDeclaredField("personName");
        Assert.assertFalse(ReflectUtils.isTransientOrStatic(field));
        
        Assert.assertFalse(ReflectUtils.hasInterface(Client.class, Person.class));
        Assert.assertTrue(ReflectUtils.hasInterface(Client.class, Client.class));
        Assert.assertTrue(ReflectUtils.hasInterface(Client.class,CoreTestClient.class));
        
        Assert.assertTrue(ReflectUtils.hasSuperClass(ClientBase.class, CoreTestClient.class));
        Assert.assertTrue(ReflectUtils.hasSuperClass(ClientBase.class, ClientBase.class));
        Assert.assertFalse(ReflectUtils.hasSuperClass(ClientBase.class, Client.class));
        
        Assert.assertEquals(ClientBase.class,ReflectUtils.stripEnhancerClass(ClientBase.class));
    }

}
