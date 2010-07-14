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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Iterates through a Jar file for each file resource
 * 
 * @author animesh.kumar
 *
 */
public class JarFileIterator implements ResourceIterator {

	JarInputStream jar;
	JarEntry next;
	Filter filter;
	boolean initial = true;
	boolean closed = false;

	public JarFileIterator(File file, Filter filter) throws IOException {
		this(new FileInputStream(file), filter);
	}

	
	public JarFileIterator(InputStream is, Filter filter) throws IOException {
		this.filter = filter;
		jar = new JarInputStream(is);
	}

	private void setNext() {
		initial = true;
		try {
			if (next != null) {
				jar.closeEntry();
			}
			next = null;

			do {
				next = jar.getNextJarEntry();
			} while (next != null && (next.isDirectory() || (filter == null || !filter.accepts(next.getName()))));

			if (next == null) {
				close();
			}
		} catch (IOException e) {
			throw new RuntimeException("failed to browse jar", e);
		}
	}

	public InputStream next() {
		if (closed || (next == null && !initial)) return null;
		setNext();
		if (next == null) return null;
		return new InputStreamWrapper(jar);
	}

	public void close() {
		try {
			closed = true;
			jar.close();
		} catch (IOException ignored) {

		}

	}

	class InputStreamWrapper extends InputStream {
		private InputStream delegate;

		public InputStreamWrapper(InputStream delegate) {
			this.delegate = delegate;
		}

		public int read() throws IOException {
			return delegate.read();
		}

		public int read(byte[] bytes) throws IOException {
			return delegate.read(bytes);
		}

		public int read(byte[] bytes, int i, int i1) throws IOException {
			return delegate.read(bytes, i, i1);
		}

		public long skip(long l) throws IOException {
			return delegate.skip(l);
		}

		public int available() throws IOException {
			return delegate.available();
		}

		public void close() throws IOException {
			// ignored
		}

		public void mark(int i) {
			delegate.mark(i);
		}

		public void reset() throws IOException {
			delegate.reset();
		}

		public boolean markSupported() {
			return delegate.markSupported();
		}
	}
}
