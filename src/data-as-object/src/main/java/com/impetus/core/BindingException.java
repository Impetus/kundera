/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.core;

/**
 * The Class BindingException.
 */
public class BindingException extends RuntimeException
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new binding exception.
     */
    public BindingException()
    {
        super();
    }

    /**
     * Instantiates a new binding exception.
     *
     * @param message
     *            the message
     */
    public BindingException(String message)
    {
        super(message);

    }

    /**
     * Instantiates a new binding exception.
     *
     * @param cause
     *            the cause
     */
    public BindingException(Throwable cause)
    {
        super(cause);

    }

    /**
     * Instantiates a new binding exception.
     *
     * @param message
     *            the message
     * @param cause
     *            the cause
     */
    public BindingException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
