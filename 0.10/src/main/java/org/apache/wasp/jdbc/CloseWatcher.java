/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.wasp.jdbc;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A phantom reference to watch for unclosed objects.
 */
public class CloseWatcher extends PhantomReference<Object> {

    /**
     * The queue (might be set to null at any time).
     */
    private static ReferenceQueue<Object> queue = new ReferenceQueue<Object>();

    /**
     * The reference set. Must keep it, otherwise the references are garbage
     * collected first and thus never enqueued.
     */
    private static Set<CloseWatcher> refs = createSet();

    /**
     * The stack trace of when the object was created. It is converted to a
     * string early on to avoid classloader problems (a classloader can't be
     * garbage collected if there is a static reference to one of its classes).
     */
    private String openStackTrace;

    /**
     * The closeable object.
     */
    private Closeable closeable;

    public CloseWatcher(Object referent, ReferenceQueue<Object> q, Closeable closeable) {
        super(referent, q);
        this.closeable = closeable;
    }

    private static Set<CloseWatcher> createSet() {
        return Collections.synchronizedSet(new HashSet<CloseWatcher>());
    }

    /**
     * Check for an collected object.
     *
     * @return the first watcher
     */
    public static CloseWatcher pollUnclosed() {
        ReferenceQueue<Object> q = queue;
        if (q == null) {
            return null;
        }
        while (true) {
            CloseWatcher cw = (CloseWatcher) q.poll();
            if (cw == null) {
                return null;
            }
            if (refs != null) {
                refs.remove(cw);
            }
            if (cw.closeable != null) {
                return cw;
            }
        }
    }

    /**
     * Register an object. Before calling this method, pollUnclosed() should be
     * called in a loop to remove old references.
     *
     * @param o the object
     * @param closeable the object to close
     * @param stackTrace whether the stack trace should be registered (this is
     *            relatively slow)
     * @return the close watcher
     */
    public static CloseWatcher register(Object o, Closeable closeable, boolean stackTrace) {
        ReferenceQueue<Object> q = queue;
        if (q == null) {
            q = new ReferenceQueue<Object>();
            queue = q;
        }
        CloseWatcher cw = new CloseWatcher(o, q, closeable);
        if (stackTrace) {
            Exception e = new Exception("Open Stack Trace");
            StringWriter s = new StringWriter();
            e.printStackTrace(new PrintWriter(s));
            cw.openStackTrace = s.toString();
        }
        if (refs == null) {
            refs = createSet();
        }
        refs.add(cw);
        return cw;
    }

    /**
     * Unregister an object, so it is no longer tracked.
     *
     * @param w the reference
     */
    public static void unregister(CloseWatcher w) {
        w.closeable = null;
        refs.remove(w);
    }

    /**
     * Get the open stack trace or null if none.
     *
     * @return the open stack trace
     */
    public String getOpenStackTrace() {
        return openStackTrace;
    }

    public Closeable getCloseable() {
        return closeable;
    }
}