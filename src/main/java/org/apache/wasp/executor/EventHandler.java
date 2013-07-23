/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wasp.executor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wasp.Server;
import org.apache.wasp.executor.ExecutorService.ExecutorType;
import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;

/**
 * Abstract base class for all wasp event handlers. Subclasses should implement
 * the {@link #process()} method. Subclasses should also do all necessary checks
 * up in their constructor if possible -- check table exists, is disabled, etc.
 * -- so they fail fast rather than later when process is running. Do it this
 * way because process be invoked directly but event handlers are also run in an
 * executor context -- i.e. asynchronously -- and in this case, exceptions
 * thrown at process time will not be seen by the invoker, not till we implement
 * a call-back mechanism so the client can pick them up later.
 * <p>
 * Event handlers have an {@link EventType}. {@link EventType} is a list of ALL
 * handler event types. We need to keep a full list in one place -- and as enums
 * is a good shorthand for an implemenations -- because event handlers can be
 * passed to executors when they are to be run asynchronously. The wasp
 * executor, see {@link ExecutorService}, has a switch for passing event type to
 * executor.
 * <p>
 * Event listeners can be installed and will be called pre- and post- process if
 * this EventHandler is run in a Thread (its a Runnable so if its {@link #run()}
 * method gets called). Implement {@link EventHandlerListener}s, and registering
 * using {@link #setListener(EventHandlerListener)}.
 * 
 * @see ExecutorService
 */
public abstract class EventHandler implements Runnable, Comparable<Runnable> {
  private static final Log LOG = LogFactory.getLog(EventHandler.class);

  // type of event this object represents
  protected EventType eventType;

  protected Server server;

  // sequence id generator for default FIFO ordering of events
  protected final static AtomicLong seqids = new AtomicLong(0);

  // sequence id for this event
  private final long seqid;

  // Listener to call pre- and post- processing. May be null.
  private EventHandlerListener listener;

  // Time to wait for events to happen, should be kept short
  protected final int waitingTimeForEvents;

  private final Span parent;

  /**
   * This interface provides pre- and post-process hooks for events.
   */
  public interface EventHandlerListener {
    /**
     * Called before any event is processed
     * 
     * @param event
     *          The event handler whose process method is about to be called.
     */
    public void beforeProcess(EventHandler event);

    /**
     * Called after any event is processed
     * 
     * @param event
     *          The event handler whose process method is about to be called.
     */
    public void afterProcess(EventHandler event);
  }

  /**
   * List of all wasp event handler types. Event types are named by a
   * convention: event type names specify the component from which the event
   * originated and then where its destined -- e.g. FSERVER2ZK_ prefix means the
   * event came from a fServer destined for zookeeper -- and then what the even
   * is; e.g. ENTITYGROUP_OPENING.
   * 
   * <p>
   * We give the enums indices so we can add types later and keep them grouped
   * together rather than have to add them always to the end as we would have to
   * if we used raw enum ordinals.
   */
  public enum EventType {
    // Messages originating from FServer (NOTE: there is NO direct communication from
    // FServer to FMaster). These are a result of FServer updates into ZK.
    FSERVER_ZK_ENTITYGROUP_CLOSED       (1, ExecutorType.MASTER_CLOSE_ENTITYGROUP),   // FServer has finished closing a entityGroup
    FSERVER_ZK_ENTITYGROUP_OPENING      (2, null), // FServer is in process of opening a entityGroup
    FSERVER_ZK_ENTITYGROUP_OPENED       (3, ExecutorType.MASTER_OPEN_ENTITYGROUP), // FServer has finished opening a entityGroup
    FSERVER_ZK_ENTITYGROUP_FAILED_OPEN  (4, ExecutorType.MASTER_CLOSE_ENTITYGROUP),   // FServer failed to open a entityGroup
    FSERVER_ZK_ENTITYGROUP_SPLITTING    (5, null), // RS has started a entityGroup split
    FSERVER_ZK_ENTITYGROUP_SPLIT        (6, ExecutorType.MASTER_SERVER_OPERATIONS),   // RS split has completed.
   
    // Messages originating from Master to FServer
    M_FSERVER_OPEN_ENTITYGROUP          (20, ExecutorType.FSERVER_OPEN_ENTITYGROUP),  // Master asking FServer to open a entityGroup
    M_FSERVER_CLOSE_ENTITYGROUP         (21, ExecutorType.FSERVER_CLOSE_ENTITYGROUP),  // Master asking FServer to close a entityGroup
 
    // Messages originating from Client to Master
    C_M_DELETE_TABLE                    (40, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to delete a table
    C_M_DISABLE_TABLE                   (41, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to disable a table
    C_M_ENABLE_TABLE                    (42, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to enable a table
    C_M_MODIFY_TABLE                    (43, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to modify a table
    C_M_ADD_COLUMN                      (44, ExecutorType.MASTER_TABLE_OPERATIONS), // Client asking Master to add column to table
    C_M_DELETE_COLUMN                   (45, ExecutorType.MASTER_TABLE_OPERATIONS), // Client asking Master to delete column of table
    C_M_MODIFY_COLUMN                   (46, ExecutorType.MASTER_TABLE_OPERATIONS), // Client asking Master to modify column of table
    C_M_CREATE_TABLE                    (47, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to create a table
    C_M_CREATE_INDEX                    (48, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to create a index
    C_M_DELETE_INDEX                    (49, ExecutorType.MASTER_TABLE_OPERATIONS),   // Client asking Master to drop a index
    C_M_TRUNCATE_TABLE(50, ExecutorType.MASTER_TABLE_OPERATIONS),
    // Client asking Master to truncate a table

    // Updates from master to ZK. This is done by the master and there is
    // nothing to process by either Master or FServer
    M_ZK_ENTITYGROUP_OFFLINE            (60, null), // Master adds this entityGroup as offline in ZK
    M_ZK_ENTITYGROUP_CLOSING            (61, null), // Master adds this entityGroup as closing in ZK

    // Master controlled events to be executed on the master
    M_SERVER_SHUTDOWN                   (80, ExecutorType.MASTER_SERVER_OPERATIONS);  // Master is processing shutdown of a FServer

    private final int code;
    private final ExecutorService.ExecutorType executor;

    /**
     * Constructor
     */
    EventType(final int code, final ExecutorType executor) {
      this.code = code;
      this.executor = executor;
    }

    public int getCode() {
      return this.code;
    }

    public static EventType get(final int code) {
      // Is this going to be slow? Its used rare but still...
      for (EventType et : EventType.values()) {
        if (et.getCode() == code)
          return et;
      }
      throw new IllegalArgumentException("Unknown code " + code);
    }

    public boolean isOnlineSchemaChangeSupported() {
      return (this.equals(EventType.C_M_MODIFY_TABLE));
    }

    ExecutorType getExecutorServiceType() {
      return this.executor;
    }
  }

  /**
   * Default base class constructor.
   */
  public EventHandler(Server server, EventType eventType) {
    this.parent = Trace.currentTrace();
    this.server = server;
    this.eventType = eventType;
    seqid = seqids.incrementAndGet();
    this.waitingTimeForEvents = server.getConfiguration().getInt(
        "wasp.master.event.waiting.time", 1000);
  }

  public void run() {
    Span chunk = Trace.startSpan(Thread.currentThread().getName(), parent,
        Sampler.ALWAYS);
    try {
      if (getListener() != null)
        getListener().beforeProcess(this);
      process();
      if (getListener() != null)
        getListener().afterProcess(this);
    } catch (Throwable t) {
      LOG.error("Caught throwable while processing event " + eventType, t);
    } finally {
      chunk.stop();
    }
  }

  /**
   * This method is the main processing loop to be implemented by the various
   * subclasses.
   * 
   * @throws IOException
   */
  public abstract void process() throws IOException;

  /**
   * Return the event type
   * 
   * @return The event type.
   */
  public EventType getEventType() {
    return this.eventType;
  }

  /**
   * Get the priority level for this handler instance. This uses natural
   * ordering so lower numbers are higher priority.
   * <p>
   * Lowest priority is Integer.MAX_VALUE. Highest priority is 0.
   * <p>
   * Subclasses should override this method to allow prioritizing handlers.
   * <p>
   * Handlers with the same priority are handled in FIFO order.
   * <p>
   * 
   * @return Integer.MAX_VALUE by default, override to set higher priorities
   */
  public int getPriority() {
    return Integer.MAX_VALUE;
  }

  /**
   * @return This events' sequence id.
   */
  public long getSeqid() {
    return this.seqid;
  }

  /**
   * Default prioritized runnable comparator which implements a FIFO ordering.
   * <p>
   * Subclasses should not override this. Instead, if they want to implement
   * priority beyond FIFO, they should override {@link #getPriority()}.
   */
  @Override
  public int compareTo(Runnable o) {
    EventHandler eh = (EventHandler) o;
    if (getPriority() != eh.getPriority()) {
      return (getPriority() < eh.getPriority()) ? -1 : 1;
    }
    return (this.seqid < eh.seqid) ? -1 : 1;
  }

  /**
   * @return Current listener or null if none set.
   */
  public synchronized EventHandlerListener getListener() {
    return listener;
  }

  /**
   * @param listener
   *          Listener to call pre- and post- {@link #process()}.
   */
  public synchronized void setListener(EventHandlerListener listener) {
    this.listener = listener;
  }

  @Override
  public String toString() {
    return "Event #" + getSeqid() + " of type " + eventType + " ("
        + getInformativeName() + ")";
  }

  /**
   * Event implementations should override thie class to provide an informative
   * name about what event they are handling. For example, event-specific
   * information such as which entityGroup or server is being processed should
   * be included if possible.
   */
  public String getInformativeName() {
    return this.getClass().toString();
  }
}
