/**
 * 
 */
package com.impetus.kundera.loader;

/**
 * @author impetus
 *
 */
public class ClientResolverException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor using fields.
	 * @param errorMsg error message.
	 */
	public ClientResolverException(String errorMsg) {
		super(errorMsg);
	}
}
