/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.kundera.client.crud.entitylisteners;

import javax.persistence.PrePersist;

/**
 * The Class Listener.
 */
public class Listener
{

    /**
     * On pre persist.
     * 
     * @param entity
     *            the entity
     */
    @PrePersist
    void onPrePersist(final Object entity)
    {

        String id = "id_set_from_pre_persist";

        ((AbstractSuperClass) entity).setExternId(id);
    }
}
