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

/**
 * @author Chhavi Gangwal
 *
 */
public final class ValidationFactoryGenerator
{
    /**
     * Defines the levels of validation factory     
     */
    public static enum ValidationFactoryType
    {
        BOOT_STRAP_VALIDATION, OPERATIONAL_VALIDATION, LISTENER_VALIDATION
    };

    /**
     * @param validatorFactoryType
     * @return
     */
    public ValidationFactory getFactory(ValidationFactoryType validatorFactoryType)
    {
        ValidationFactory validationFactory = null;
        switch (validatorFactoryType)
        {
        case BOOT_STRAP_VALIDATION:
            validationFactory = new BootstrapValidationFactory();
            break;
        case OPERATIONAL_VALIDATION:
            validationFactory = new OperationValidationFactory();
            break;
        case LISTENER_VALIDATION:
            validationFactory = new ListenerValidationFactory();
            break;
        }
        return validationFactory;

    }

}
