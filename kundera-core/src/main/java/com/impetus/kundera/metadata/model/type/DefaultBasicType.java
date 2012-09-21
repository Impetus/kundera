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
package com.impetus.kundera.metadata.model.type;

import javax.persistence.metamodel.BasicType;
import javax.persistence.metamodel.Type;

/**
 * Default implementation of {@link BasicType}
 * 
 * <code> DefaultBasicType</code> implements <code>BasicType</code> interface,
 * invokes constructor with PersistenceType.BASIC. Default implementation of
 * {@link Type} interface is provided by {@link AbstractType}
 * 
 * @author vivek.mishra
 * 
 */
public class DefaultBasicType<X> extends AbstractType<X> implements BasicType<X>
{

    /**
     * Constructor with arguments.
     * 
     * @param clazz
     *            typed class.
     */
    public DefaultBasicType(Class<X> clazz)
    {
        super(clazz, PersistenceType.BASIC);
    }

}
