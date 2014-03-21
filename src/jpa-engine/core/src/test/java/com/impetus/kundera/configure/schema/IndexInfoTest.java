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
package com.impetus.kundera.configure.schema;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author vivek.mishra
 * junit for {@link IndexInfo}
 *
 */
public class IndexInfoTest
{

    @Test
    public void testWithSingleValueConstructor()
    {
        IndexInfo indexInfo = new IndexInfo("personName");
        indexInfo.setIndexType("KEYS");
        indexInfo.setMaxValue(12);
        indexInfo.setMinValue(1);
        
        Assert.assertEquals(1, indexInfo.getMinValue().intValue());
        Assert.assertEquals(1, indexInfo.getMinValue().intValue());
        Assert.assertEquals("KEYS", indexInfo.getIndexType());
        
        Assert.assertEquals("personName", indexInfo.getColumnName());
        
    }

    @Test
    public void testWithMultiValueConstructor()
    {
        IndexInfo indexInfo = new IndexInfo("personName",12,1,"KEYS", "personName");
        indexInfo.setIndexType("KEYS");
        indexInfo.setMaxValue(12);
        indexInfo.setMinValue(1);
        
        Assert.assertEquals(12, indexInfo.getMaxValue().intValue());
        Assert.assertEquals(1, indexInfo.getMinValue().intValue());
        Assert.assertEquals("KEYS", indexInfo.getIndexType());
        
        Assert.assertEquals("personName", indexInfo.getColumnName());
        
    }


}
