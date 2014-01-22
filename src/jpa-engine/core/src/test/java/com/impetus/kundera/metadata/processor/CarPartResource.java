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
import javax.persistence.Entity;




/**
 * @author impadmin
 * 
 */
@Entity
public class CarPartResource extends AbstractResource
{
    @Column
    private String col;
    
    @Embedded
    private CarTyre tyre;

    public String getCol()
    {
        return col;
    }

    public void setCol(String col)
    {
        this.col = col;
    }
    
  

}