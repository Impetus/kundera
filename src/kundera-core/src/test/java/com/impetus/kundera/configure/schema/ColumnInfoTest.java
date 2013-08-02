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
 * junit for {@link ColumnInfo}
 *
 */
public class ColumnInfoTest
{

    @Test
    public void testWithSingleValueConstructor()
    {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setColumnName("column1");
        columnInfo.setIndexable(true);
        columnInfo.setType(Integer.class);
        
        Assert.assertEquals("column1", columnInfo.getColumnName());
        Assert.assertTrue(columnInfo.isIndexable());
        Assert.assertEquals(Integer.class, columnInfo.getType());
        
        ColumnInfo col1 = new ColumnInfo();
        col1.setColumnName("column1");
        
        Assert.assertEquals(col1, columnInfo);  //column name comparison.
        
        Assert.assertNotNull(col1.toString());
        
    }

}
