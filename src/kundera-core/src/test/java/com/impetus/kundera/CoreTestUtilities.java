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

package com.impetus.kundera;

import java.lang.reflect.Field;

import javax.persistence.EntityManager;
import javax.persistence.Parameter;

import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * @author vivek.mishra
 * Test utility to serve generic utility method required by various kundera-core junits.
 */
public final class CoreTestUtilities
{

    /**
     * Returns persistence delegator instance for provided entity manager instance.
     * 
     * @param em
     * @return
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public final static PersistenceDelegator getDelegator(EntityManager em) throws NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Field pdField = em.getClass().getDeclaredField("persistenceDelegator");
        if (!pdField.isAccessible())
        {
            pdField.setAccessible(true);
        }

        PersistenceDelegator delegator = (PersistenceDelegator) pdField.get(em);
        return delegator;

    }

    public static Parameter getParameter()
    {
        return new CoreTestUtilities.JPAParameter();
    }
    
    public static Parameter getParameter(final String name, Object value)
    {
        return new CoreTestUtilities.JPAParameter(name,value);
    }

    public static Parameter getParameter(final int position, Object value)
    {
        return new CoreTestUtilities.JPAParameter(position,value);
    }

    private static class JPAParameter implements Parameter<String>
    {
        private String name = "jpa";
        
        private int position;
        
        private Object value;

        private JPAParameter()
        {
        }
        
        private JPAParameter(final String paramName, Object value)
        {
            this.name = paramName;
            this.value = value;
        }
        
        private JPAParameter(final int position, Object value)
        {
            this.position = position;
            this.value = value;
        }
        
        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public Integer getPosition()
        {
            return this.position;
        }

        
        @Override
        public Class<String> getParameterType()
        {
            return String.class;
        }
        
    }

}
