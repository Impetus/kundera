/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.validation;

import com.impetus.kundera.validation.rules.RuleValidationException;

/**
 * @author Chhavi Gangwal
 *
 */
public class ListenerValidationFactory extends AbstractValidationFactory implements ValidationFactory
{
        /* (non-Javadoc)
         * @see com.impetus.kundera.validation.AbstractValidationFactory#validate(java.lang.Class)
         */
        @Override
        public boolean validate(Class clazz) throws RuleValidationException
        {
            throw new UnsupportedOperationException("Listener level validations are not supported!");
        }

   

}
