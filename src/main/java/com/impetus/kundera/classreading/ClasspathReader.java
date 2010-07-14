/*
 * Copyright (c) 2010-2011, Animesh Kumar
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impetus.kundera.classreading;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author animesh.kumar
 * 
 */
public class ClasspathReader extends Reader {

	protected transient String[] ignoredPackages = { "javax", "java", "sun", "com.sun", "javassist" };

	Filter filter;

	public ClasspathReader() {
		filter = new FilterImpl();
	}

	@Override
	public void read () {
		URL[] resources = findResources ();
		for (URL resource : resources) {
			try {
				ResourceIterator itr = getResourceIterator(resource, getFilter());
				
				InputStream is = null;
				while ((is = itr.next()) != null) {
					scanClass(is);
				}
			} catch (IOException e) {
				// TODO: Do something with this exception
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Uses the java.class.path system property to obtain a list of URLs that
	 * represent the CLASSPATH
	 * 
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public URL[] findResources() {
		List<URL> list = new ArrayList<URL>();
		String classpath = System.getProperty("java.class.path");
		StringTokenizer tokenizer = new StringTokenizer(classpath, File.pathSeparator);

		while (tokenizer.hasMoreTokens()) {
			String path = tokenizer.nextToken();

			File fp = new File(path);
			if (!fp.exists()) throw new RuntimeException("File in java.class.path does not exist: " + fp);
			try {
				list.add(fp.toURL());
			} catch (MalformedURLException e) {
				throw new RuntimeException(e);
			}
		}
		return list.toArray(new URL[list.size()]);
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}
}
