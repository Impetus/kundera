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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.annotation.Annotation;

/**
 * Base class-reader
 * 
 * @author animesh.kumar
 *
 */
public abstract class Reader {

	private List<String> validAnnotations = new ArrayList<String>();
	private List<AnnotationDiscoveryListener> annotationDiscoveryListeners = new ArrayList<AnnotationDiscoveryListener>();
	
	public Reader () {
	}
	
	public void scanClass(InputStream bits) throws IOException {
		DataInputStream dstream = new DataInputStream(new BufferedInputStream(bits));
		ClassFile cf = null;
		try {
			cf = new ClassFile(dstream);
			
			String className = cf.getName();
			List<String> annotations = new ArrayList<String>();
			
			accumulateAnnotations(annotations, (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.visibleTag));
			accumulateAnnotations(annotations, (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.invisibleTag));
			
			// iterate through all valid annotations
			for (String validAnn : getValidAnnotations()) {
				// check if the current class has one?
				if (annotations.contains(validAnn)) {
					// fire all listeners
					for (AnnotationDiscoveryListener listener : getAnnotationDiscoveryListeners()) {
						listener.discovered(className, annotations.toArray(new String[] {}));
					}
				}
			}
			
		} finally {
			dstream.close();
			bits.close();
		}
	}
	
	// helper method to accumulate annotations.
	private void accumulateAnnotations (List<String> annotations, AnnotationsAttribute annatt) {
		if (null == annatt) return;
		for (Annotation ann : annatt.getAnnotations()) {
			annotations.add(ann.getTypeName());
		}
	}
	
	public ResourceIterator getResourceIterator(URL url, Filter filter) throws IOException {
		String urlString = url.toString();
		if (urlString.endsWith("!/")) {
			urlString = urlString.substring(4);
			urlString = urlString.substring(0, urlString.length() - 2);
			url = new URL(urlString);
		}

		if (!urlString.endsWith("/")) {
			return new JarFileIterator(url.openStream(), filter);
		} else {

			if (!url.getProtocol().equals("file")) {
				throw new IOException("Unable to understand protocol: " + url.getProtocol());
			}

			File f = new File(url.getPath());
			if (f.isDirectory()) {
				return new ClassFileIterator(f, filter);
			} else {
				return new JarFileIterator(url.openStream(), filter);
			}
		}
	}
	
	public List<String> getValidAnnotations () {
		return validAnnotations;
	}

	public void addValidAnnotations (String annotation) {
		this.validAnnotations.add(annotation);
	}

	public List<AnnotationDiscoveryListener> getAnnotationDiscoveryListeners() {
		return annotationDiscoveryListeners;
	}
	
	public void addAnnotationDiscoveryListeners(AnnotationDiscoveryListener annotationDiscoveryListener) {
		this.annotationDiscoveryListeners.add(annotationDiscoveryListener);
	}

	public abstract void read();
	
	public abstract Filter getFilter();

}
