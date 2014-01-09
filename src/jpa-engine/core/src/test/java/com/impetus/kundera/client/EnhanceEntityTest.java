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
package com.impetus.kundera.client;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.persistence.event.AddressEntity;

/**
 * @author vivek.mishra
 * junit for {@link EnhanceEntity}
 */
public class EnhanceEntityTest
{

    @Test
    public void test()
    {
        AddressEntity address = new AddressEntity();
        address.setAddressId("addr1");
        address.setCity("noida");
        EnhanceEntity enhanceEntity  = new EnhanceEntity(address,"addr1",null);
        Assert.assertNotNull(enhanceEntity);
        Assert.assertEquals("addr1", enhanceEntity.getEntityId());
        Assert.assertEquals(address, enhanceEntity.getEntity());
        Assert.assertNull(enhanceEntity.getRelations());
        enhanceEntity = new EnhanceEntity();
        Assert.assertNull(enhanceEntity.getEntity());
    }

}
