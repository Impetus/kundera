package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;

/**
 * Contains Index information of a field.
 * 
 * @author animesh.kumar
 */
public final class PropertyIndex {

	/** The name. */
	private String name;

	/** The property. */
	private Field property;

	/** The boost. */
	private float boost = 1.0f;

	/**
	 * The Constructor.
	 * 
	 * @param property
	 *            the property
	 */
	public PropertyIndex(Field property) {
		this.property = property;
		this.name = property.getName();
	}

	/**
	 * Instantiates a new property index.
	 * 
	 * @param property
	 *            the property
	 * @param name
	 *            the name
	 */
	public PropertyIndex(Field property, String name) {
		this.property = property;
		this.name = name;
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the property.
	 * 
	 * @return the property
	 */
	public Field getProperty() {
		return property;
	}

	/**
	 * Gets the boost.
	 * 
	 * @return the boost
	 */
	public float getBoost() {
		return boost;
	}

	/**
	 * Sets the boost.
	 * 
	 * @param boost
	 *            the new boost
	 */
	public void setBoost(float boost) {
		this.boost = boost;
	}
}