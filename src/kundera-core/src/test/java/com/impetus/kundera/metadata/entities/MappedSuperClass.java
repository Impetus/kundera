package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;

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

/**
 * @author vivek.mishra
 * 
 */
@MappedSuperclass
public class MappedSuperClass extends RootMappedSuperClass
{
    @Column
    private int mappedInt;

    @Column
    private Float mappedFloat;

    /**
     * @return the mappedInt
     */
    public int getMappedInt()
    {
        return mappedInt;
    }

    /**
     * @param mappedInt
     *            the mappedInt to set
     */
    public void setMappedInt(int mappedInt)
    {
        this.mappedInt = mappedInt;
    }

    /**
     * @return the mappedFloat
     */
    public Float getMappedFloat()
    {
        return mappedFloat;
    }

    /**
     * @param mappedFloat
     *            the mappedFloat to set
     */
    public void setMappedFloat(Float mappedFloat)
    {
        this.mappedFloat = mappedFloat;
    }

}
