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

import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * 
 * Search result test junit test. Such POJO junits are just for code coverage.
 */
public class SearchResultTest
{

    @Test
    public void test()
    {
        Person p = new Person();
        SearchResult result = new SearchResult();
        
        result.setPrimaryKey(p.getPersonId());
        result.setEmbeddedColumnName("none");
        result.addEmbeddedColumnValue("embeddedcolumn1");
        result.addEmbeddedColumnValue("embeddedcolumn2");
        
        Assert.assertNull(result.getPrimaryKey());
        Assert.assertNotNull(result.getEmbeddedColumnName());
        Assert.assertNotNull(result.getEmbeddedColumnValues());
        Assert.assertEquals(2,result.getEmbeddedColumnValues().size());
        
    }

}
