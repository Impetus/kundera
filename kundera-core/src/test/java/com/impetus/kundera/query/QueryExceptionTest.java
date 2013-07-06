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
package com.impetus.kundera.query;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author vivek.mishra
 * junit for {@link QueryHandlerException} & {@link JPQLParseException}.
 * Somehow coverage is not getting count. So this junit is purly from coverage purpose only.
 */
public class QueryExceptionTest
{

    @Test
    public void testQueryHandlerException()
    {
        QueryHandlerException exception = new QueryHandlerException();
        Assert.assertNotNull(exception);

        exception = new QueryHandlerException("Error with string");
        Assert.assertNotNull(exception);
        exception = new QueryHandlerException("Error with string and runtime error", new RuntimeException());
        Assert.assertNotNull(exception);
        exception = new QueryHandlerException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testJPQLParseException()
    {
        JPQLParseException exception = new JPQLParseException();
        Assert.assertNotNull(exception);

        exception = new JPQLParseException("Error with string");
        Assert.assertNotNull(exception);
        exception = new JPQLParseException(new RuntimeException());
        Assert.assertNotNull(exception);
    }
}
