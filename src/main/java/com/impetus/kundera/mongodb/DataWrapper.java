/**
 * 
 */
package com.impetus.kundera;

/**
 * @author impetus
 *
 */
public interface DataWrapper {
	
	/**
	 * Returns column family.
	 * @return column family.
	 */
	String getColumnFamily() ;
	
	/**
	 * Returns row key. 
	 * @return rowKey.
	 */
	String getRowKey();

}
