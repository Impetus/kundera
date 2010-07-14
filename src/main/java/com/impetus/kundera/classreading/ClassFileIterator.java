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
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author animesh.kumar
 * 
 */
public class ClassFileIterator implements ResourceIterator {

	protected transient String[] ignoredPackages = { "javax", "java", "sun", "com.sun", "javassist" };

	private ArrayList<File> files;
	private int index = 0;

	public ClassFileIterator(File file, Filter filter) {
		files = new ArrayList<File>();
		try {
			create(files, file, filter);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void create(List<File> list, File dir, Filter filter) throws Exception {
		File[] files = dir.listFiles();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory()) {
				create(list, files[i], filter);
			} else {
				if (filter == null || filter.accepts(files[i].getAbsolutePath())) {
					list.add(files[i]);
				}
			}
		}
	}

	public InputStream next() {
		if (index >= files.size()) return null;
		File fp = (File) files.get(index++);
		try {
			return new FileInputStream(fp);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
	}
}
