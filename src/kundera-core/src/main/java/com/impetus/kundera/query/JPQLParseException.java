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
package com.impetus.kundera.query;

/**
 * @author amresh
 * 
 */
public class JPQLParseException extends QueryHandlerException
{

    /**
     * 
     */
    public JPQLParseException()
    {
        super();
    }

    /**
     * @param paramThrowable
     */
    public JPQLParseException(Throwable paramThrowable)
    {
        super(paramThrowable);
    }

    /**
     * @param errMsg
     */
    public JPQLParseException(String errMsg)
    {
        super(
                errMsg
                        + ". For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf");
    }

}
