/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.metadata.processor;


import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * @author vivek.mishra Transaction {@link MappedSuperclass}
 */

@MappedSuperclass
public abstract class AbstractResource
{

    @Id
    private String cprId;

    @Column
    private String resourceName;
    
    @Embedded
    private CarEngine engine;

    public String getCarPartResourceId()
    {
        return cprId;
    }

    public void setCarPartResourceId(String cprId)
    {
        this.cprId = cprId;
    }

    public String getCarPartResourceName()
    {
        return resourceName;
    }

    public void setCarPartResourceName(String resourceName)
    {
        this.resourceName = resourceName;
    }
    
    public CarEngine getEngine()
    {
        return engine;
    }

    public void setEngine(CarEngine engine)
    {
        this.engine = engine;
    }

}
