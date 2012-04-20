/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

import com.impetus.kundera.KunderaException;

/**
 * The Class ReaderResolverException.
 * 
 * @author vivek.mishra
 */
public class ReaderResolverException extends KunderaException
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -348290906946430547L;

    /**
     * 
     */
    public ReaderResolverException()
    {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public ReaderResolverException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public ReaderResolverException(String arg0)
    {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public ReaderResolverException(Throwable arg0)
    {
        super(arg0);
    }

}
