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
package com.impetus.kundera.classreading;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.persistence.event.AddressEntity;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * Relation holder junit test
 */
public class RelationHolderTest
{

    @Test
    public void test()
    {
        Person p = new Person();
        RelationHolder rlHolder = new RelationHolder("parent", p);
        
        Assert.assertEquals("parent", rlHolder.getRelationName());
        Assert.assertEquals(p, rlHolder.getRelationValue());
        
        AddressEntity relationEntity = new AddressEntity();
        rlHolder = new RelationHolder("child", p, relationEntity);
        Assert.assertEquals("child", rlHolder.getRelationName());
        Assert.assertEquals(p, rlHolder.getRelationValue());
        Assert.assertEquals(relationEntity, rlHolder.getRelationVia());

    }

}
