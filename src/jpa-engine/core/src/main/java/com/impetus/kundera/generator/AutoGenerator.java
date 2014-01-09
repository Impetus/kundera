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
package com.impetus.kundera.generator;

/**
 * {@link AutoGenerator} interface , all client should implement this interface
 * in order to support auto generation strategy.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public interface AutoGenerator extends Generator
{
    /**
     * generate id, Its totally client responsibility to generate Id, using
     * client specific strategy.
     * 
     * @param discriptor
     * @return
     */
    public Object generate();
}
