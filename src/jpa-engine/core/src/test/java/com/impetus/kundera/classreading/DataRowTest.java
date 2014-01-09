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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * 
 * Data row test junit test. Such POJO junits are just to code coverage happy.
 */
public class DataRowTest
{

    @Test
    public void test()
    {
        Person p = new Person();
        
        List<String> columns = new ArrayList<String>();
        columns.add("column1");
        columns.add("column2");
        columns.add("column3");
        
        DataRow<String> row = new DataRow<String>("1","person",columns);
        
        
        Assert.assertNotNull(row.getColumnFamilyName());
        Assert.assertNotNull(row.getId());
        Assert.assertNotNull(row.getColumns());
        Assert.assertEquals(3,row.getColumns().size());
        
        row.addColumn("column4");
        Assert.assertEquals(4,row.getColumns().size());

    }

}
