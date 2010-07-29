/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.ejb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.persistence.spi.PersistenceUnitTransactionType;

/**
 * PersistenceMetadata class
 * 
 * @author animesh.kumar
 *
 */
public class PersistenceMetadata {
	
	private String name;
	private String provider;
	private PersistenceUnitTransactionType transactionType;
	private List<String> classes = new ArrayList<String>();
	private List<String> packages = new ArrayList<String>();
	private Set<String> jarFiles = new HashSet<String>();
	private Properties props = new Properties();
	private boolean excludeUnlistedClasses = false;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public PersistenceUnitTransactionType getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(PersistenceUnitTransactionType transactionType) {
		this.transactionType = transactionType;
	}

	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		if ( provider != null && provider.endsWith( ".class" ) ) {
			this.provider = provider.substring( 0, provider.length() - 6 );
		}
		this.provider = provider;
	}

	public List<String> getClasses() {
		return classes;
	}

	public void setClasses(List<String> classes) {
		this.classes = classes;
	}

	public List<String> getPackages() {
		return packages;
	}

	public void setPackages(List<String> packages) {
		this.packages = packages;
	}

	public Set<String> getJarFiles() {
		return jarFiles;
	}

	public void setJarFiles(Set<String> jarFiles) {
		this.jarFiles = jarFiles;
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}

	public boolean getExcludeUnlistedClasses() {
		return excludeUnlistedClasses;
	}

	public void setExcludeUnlistedClasses(boolean excludeUnlistedClasses) {
		this.excludeUnlistedClasses = excludeUnlistedClasses;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PersistenceMetadata [name=");
		builder.append(name);
		builder.append(", provider=");
		builder.append(provider);
		builder.append(", transactionType=");
		builder.append(transactionType);
		builder.append(", classes=");
		builder.append(classes);
		builder.append(", excludeUnlistedClasses=");
		builder.append(excludeUnlistedClasses);
		builder.append(", jarFiles=");
		builder.append(jarFiles);
		builder.append(", packages=");
		builder.append(packages);
		builder.append(", props=");
		builder.append(props);
		builder.append("]");
		return builder.toString();
	}

}
