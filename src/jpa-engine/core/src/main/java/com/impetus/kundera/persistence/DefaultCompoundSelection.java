/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.List;

import javax.persistence.criteria.CompoundSelection;
import javax.persistence.criteria.Selection;

/**
 * Implementation class for multi column selection {@link CompoundSelection}
 * @author vivek.mishra
 *
 */
public class DefaultCompoundSelection<X> implements CompoundSelection<X>
{

    private List<Selection<?>> selections;
    private Class<? extends X> resultType;
    private String alias;

    DefaultCompoundSelection(List<Selection<?>> selections, Class resultClazz)
    {
        this.selections = selections;
        this.resultType = resultClazz;
        if(isCompoundSelection())
        {
            this.alias = selections.get(0).getAlias();
        }
    }
    
    @Override
    public Selection<X> alias(String paramString)
    {
        this.alias = paramString;
        return this;
    }

    @Override
    public boolean isCompoundSelection()
    {
        return this.selections != null && !this.selections.isEmpty() && this.selections.size() > 1;
    }

    @Override
    public List<Selection<?>> getCompoundSelectionItems()
    {
        return this.selections;
    }

    @Override
    public Class<? extends X> getJavaType()
    {
        return this.resultType;
    }

    @Override
    public String getAlias()
    {
        return this.alias;
    }

}
