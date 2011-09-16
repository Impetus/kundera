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
package com.impetus.kundera;

/**
 * Constants.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public final class Constants {

	/**
	 * Instantiates a new constants.
	 */
	private Constants() {

	}

	/** The Constant ENCODING. */
	public static final String ENCODING = "utf-8";

	/** The Constant SEPARATOR. */
	public final static String SEPARATOR = "~";

	/** The Constant INVALID. */
	public final static int INVALID = -1;

	/** The Constant SUPER_COLUMN_NAME_DELIMITER. */
	public final static String EMBEDDED_COLUMN_NAME_DELIMITER = "#";

	/** The Constant TO_ONE_SUPER_COL_NAME. */
	public static final String FOREIGN_KEY_EMBEDDED_COLUMN_NAME = "FKey-TO";

}
