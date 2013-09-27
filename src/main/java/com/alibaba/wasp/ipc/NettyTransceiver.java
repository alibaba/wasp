/**
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

package com.alibaba.wasp.ipc;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ipc.NettyTransportCodec.NettyDataPack;
import com.alibaba.wasp.ipc.NettyTransportCodec.NettyFrameDecoder;
import com.alibaba.wasp.ipc.NettyTransportCodec.NettyFrameEncoder;
import com.alibaba.wasp.protobuf.generated.RPCProtos;
import com.alibaba.wasp.protobuf.generated.RPCProtos.*;
import com.alibaba.wasp.protobuf.generated.RPCProtos.RpcResponseHeader.Status;
import com.alibaba.wasp.protobuf.generated.Tracing.RPCTInfo;
import com.alibaba.wasp.util.ByteBufferInputStream;
import com.alibaba.wasp.util.ByteBufferOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.handler.timeout.WriteTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Netty-based {@link Transceiver} implementation.
 */
public class NettyTransceiver extends Transceiver {
  public static final String NETTY_CONNECT_TIMEOUT_OPTION = "connectTimeoutMillis";
  public static final String NETTY_TCP_NODELAY_OPTION = "tcpNoDelay";
  public static final boolean DEFAULT_TCP_NODELAY_VALUE = true;

  private static final Logger LOG = LoggerFactory
      .getLogger(NettyTransceiver.class.getName());

  private final AtomicInteger serialGenerator = new AtomicInteger(0);
  private final Map<Integer, Callback<List<ByteBuffer>>> requests = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>();

  private final ChannelFactory channelFactory;
  private final long connectTimeoutMillis;
  private final ClientBootstrap bootstrap;
  private final InetSocketAddress remoteAddr;

  private boolean connectionEstablished = false;

  private Object connected = new Object();

  private Map<String, Object> nettyClientBootstrapOptions;

  private int refCount = 1;

  private Configuration conf;

  private Timer timer;
  /**
   * Read lock must be acquired whenever using non-final state. Write lock must
   * be acquired whenever modifying state.
   */
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private Channel channel; // Synchronized on stateLock

  NettyTransceiver() {
    channelFactory = null;
    connectTimeoutMillis = 0L;
    bootstrap = null;
    remoteAddr = null;
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection
   * timeout.
   *
   * @param addr
   *          the address to connect to.
   * @throws java.io.IOException
   *           if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr) throws IOException {
    this(addr, FConstants.DEFAULT_CONNECTION_TIMEOUT_MILLIS);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   *
   * @param addr
   *          the address to connect to.
   * @param connectTimeoutMillis
   *          maximum amount of time to wait for connection establishment in
   *          milliseconds, or null to use
   *          {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws java.io.IOException
   *           if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, Long connectTimeoutMillis)
      throws IOException {
    this(addr, new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory("Wasp "
            + NettyTransceiver.class.getSimpleName() + " Boss")),
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory("Wasp "
            + NettyTransceiver.class.getSimpleName() + " I/O Worker"))),
        connectTimeoutMillis);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection
   * timeout.
   *
   * @param addr
   *          the address to connect to.
   * @param channelFactory
   *          the factory to use to create a new Netty Channel.
   * @throws java.io.IOException
   *           if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory)
      throws IOException {
    this(addr, channelFactory, buildDefaultBootstrapOptions(null));
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   *
   * @param addr
   *          the address to connect to.
   * @param channelFactory
   *          the factory to use to create a new Netty Channel.
   * @param connectTimeoutMillis
   *          maximum amount of time to wait for connection establishment in
   *          milliseconds, or null to use
   *          {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws java.io.IOException
   *           if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr,
      ChannelFactory channelFactory, Long connectTimeoutMillis)
      throws IOException {
    this(addr, channelFactory,
        buildDefaultBootstrapOptions(connectTimeoutMillis));
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * It is strongly recommended that the {@link #NETTY_CONNECT_TIMEOUT_OPTION}
   * option be set to a reasonable timeout value (a Long value in milliseconds)
   * to prevent connect/disconnect attempts from hanging indefinitely. It is
   * also recommended that the {@link #NETTY_TCP_NODELAY_OPTION} option be set
   * to true to minimize RPC latency.
   *
   * @param addr
   *          the address to connect to.
   * @param channelFactory
   *          the factory to use to create a new Netty Channel.
   * @param nettyClientBootstrapOptions
   *          map of Netty ClientBootstrap options to use.
   * @throws java.io.IOException
   *           if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr,
      ChannelFactory channelFactory,
      Map<String, Object> nettyClientBootstrapOptions) throws IOException {
    if (channelFactory == null) {
      throw new NullPointerException("channelFactory is null");
    }

    // Set up.
    this.channelFactory = channelFactory;
    this.connectTimeoutMillis = (Long) nettyClientBootstrapOptions
        .get(NETTY_CONNECT_TIMEOUT_OPTION);
    bootstrap = new ClientBootstrap(channelFactory);
    remoteAddr = addr;
    this.nettyClientBootstrapOptions = nettyClientBootstrapOptions;
  }

  public synchronized void connect() throws IOException {
    if (connectionEstablished) {
      return;
    }

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        timer = new HashedWheelTimer();
        p.addLast("frameDecoder", new NettyFrameDecoder());
        p.addLast("frameEncoder", new NettyFrameEncoder());
        p.addLast("handler", new NettyClientWaspHandler());
        p.addLast(
            "readTimeout",
            new ReadTimeoutHandler(timer, NettyTransceiver.this.conf.getInt(
                FConstants.CONNECTION_READ_TIMEOUT_SEC,
                FConstants.DEFAULT_CONNECTION_READ_TIMEOUT_SEC)));
        p.addLast(
            "writeTimeout",
            new WriteTimeoutHandler(timer, NettyTransceiver.this.conf.getInt(
                FConstants.CONNECTION_WRITE_TIMEOUT_SEC,
                FConstants.DEFAULT_CONNECTION_WRITE_TIMEOUT_SEC)));
        return p;
      }
    });

    if (nettyClientBootstrapOptions != null) {
      LOG.debug("Using Netty bootstrap options: " + nettyClientBootstrapOptions);
      bootstrap.setOptions(nettyClientBootstrapOptions);
    }

    // Make a new connection.
    stateLock.readLock().lock();
    try {
      getChannel();
    } finally {
      stateLock.readLock().unlock();
    }
    connectionEstablished = true;
  }

  /**
   * Creates the default options map for the Netty ClientBootstrap.
   *
   * @param connectTimeoutMillis
   *          connection timeout in milliseconds, or null if no timeout is
   *          desired.
   * @return the map of Netty bootstrap options.
   */
  private static Map<String, Object> buildDefaultBootstrapOptions(
      Long connectTimeoutMillis) {
    Map<String, Object> options = new HashMap<String, Object>(2);
    options.put(NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
    options
        .put(
            NETTY_CONNECT_TIMEOUT_OPTION,
            connectTimeoutMillis == null ? FConstants.DEFAULT_CONNECTION_TIMEOUT_MILLIS
                : connectTimeoutMillis);
    return options;
  }

  /**
   * Tests whether the given channel is ready for writing.
   *
   * @return true if the channel is open and ready; false otherwise.
   */
  private static boolean isChannelReady(Channel channel) {
    return (channel != null) && channel.isOpen() && channel.isBound()
        && channel.isConnected();
  }

  /**
   * Gets the Netty channel. If the channel is not connected, first attempts to
   * connect. NOTE: The stateLock read lock *must* be acquired before calling
   * this method.
   *
   * @return the Netty channel
   * @throws java.io.IOException
   *           if an error occurs connecting the channel.
   */
  private Channel getChannel() throws IOException {
    if (!isChannelReady(channel)) {
      // Need to reconnect
      // Upgrade to write lock
      stateLock.readLock().unlock();
      stateLock.writeLock().lock();
      try {
        if (!isChannelReady(channel)) {
          LOG.debug("Connecting to " + remoteAddr);
          ChannelFuture channelFuture = bootstrap.connect(remoteAddr);
          channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future)
                throws Exception {
              synchronized (connected) {
                if (future.isSuccess()) {
                  LOG.info("Successfully connected to bookie: " + remoteAddr);
                  channel = future.getChannel();
                } else {
                  channel = null;
                  throw new IOException("Error connecting to " + remoteAddr,
                      future.getCause());
                }
                connected.notify();
              }
            }
          });
          try {
            synchronized (connected) {
              connected.wait(connectTimeoutMillis);
            }
            if (channel == null) {
              throw new IOException("Error connecting to " + remoteAddr);
            }
          } catch (InterruptedException e) {
            throw new InternalError();
          }
        }
      } finally {
        // Downgrade to read lock:
        stateLock.readLock().lock();
        stateLock.writeLock().unlock();
      }
    }
    return channel;
  }

  /**
   * Closes the connection to the remote peer if connected.
   *
   * @param awaitCompletion
   *          if true, will block until the close has completed.
   * @param cancelPendingRequests
   *          if true, will drain the requests map and send an IOException to
   *          all Callbacks.
   * @param cause
   *          if non-null and cancelPendingRequests is true, this Throwable will
   *          be passed to all Callbacks.
   */
  private void disconnect(boolean awaitCompletion,
      boolean cancelPendingRequests, Throwable cause) {
    Channel channelToClose = null;
    Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;
    boolean stateReadLockHeld = stateLock.getReadHoldCount() != 0;
    if (stateReadLockHeld) {
      stateLock.readLock().unlock();
    }
    stateLock.writeLock().lock();
    try {
      if (channel != null) {
        if (cause != null) {
          LOG.debug("Disconnecting from " + remoteAddr, cause);
        } else {
          LOG.debug("Disconnecting from " + remoteAddr);
        }
        channelToClose = channel;
        channel = null;
        if (cancelPendingRequests) {
          // Remove all pending requests (will be canceled after relinquishing
          // write lock).
          requestsToCancel = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>(
              requests);
          requests.clear();
        }
      }
    } finally {
      if (stateReadLockHeld) {
        stateLock.readLock().lock();
      }
      stateLock.writeLock().unlock();
    }

    // Cancel any pending requests by sending errors to the callbacks:
    if ((requestsToCancel != null) && !requestsToCancel.isEmpty()) {
      LOG.debug("Removing " + requestsToCancel.size() + " pending request(s).");
      for (Callback<List<ByteBuffer>> request : requestsToCancel.values()) {
        request.handleError(cause != null ? cause : new IOException(getClass()
            .getSimpleName() + " closed"));
      }
    }

    // Close the channel:
    if (channelToClose != null) {
      ChannelFuture closeFuture = channelToClose.close();
      timer.stop();
      if (awaitCompletion && (closeFuture != null)) {
        closeFuture.awaitUninterruptibly(connectTimeoutMillis);
      }
    }
    this.connectionEstablished = false;
  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This
   * method is a no-op.
   */
  @Override
  public void lockChannel() {

  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This
   * method is a no-op.
   */
  @Override
  public void unlockChannel() {

  }

  public void close() {
    try {
      // Close the connection:
      disconnect(true, true, null);
    } finally {
      // Shut down all thread pools to exit.
      channelFactory.releaseExternalResources();
    }
  }

  @Override
  public String getRemoteName() throws IOException {
    stateLock.readLock().lock();
    try {
      return getChannel().getRemoteAddress().toString();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value. Throws
   * exceptions if there are network problems or if the remote code threw an
   * exception.
   */
  public Message call(RpcRequestBody param, InetSocketAddress addr,
      Class<? extends VersionedProtocol> protocol, int rpcTimeout)
      throws InterruptedException, IOException {
    if (!connectionEstablished) {
      connect();
    }
    ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setProtocol(protocol == null ? "" : protocol.getName());
    ConnectionHeader connectionHeader = builder.build();

    RpcRequestHeader.Builder headerBuilder = RPCProtos.RpcRequestHeader
        .newBuilder();

    if (Trace.isTracing()) {
      Span s = Trace.currentTrace();
      headerBuilder.setTinfo(RPCTInfo.newBuilder().setParentId(s.getSpanId())
          .setTraceId(s.getTraceId()));
    }
    RpcRequestHeader rpcHeader = headerBuilder.build();

    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    connectionHeader.writeDelimitedTo(bbo);
    rpcHeader.writeDelimitedTo(bbo);
    param.writeDelimitedTo(bbo);

    List<ByteBuffer> res = transceive(bbo.getBufferList());

    return processResponse(res, protocol, param);
  }

  private Message processResponse(List<ByteBuffer> res,
      Class<? extends VersionedProtocol> protocol, RpcRequestBody param)
      throws IOException {
    ByteBufferInputStream in = new ByteBufferInputStream(res);
    try {
      // See NettyServer.prepareResponse for where we write out the response.
      // It writes the call.id (int), a boolean signifying any error (and if
      // so the exception name/trace), and the response bytes

      // Read the call id.
      RpcResponseHeader response = RpcResponseHeader.parseDelimitedFrom(in);
      if (response == null) {
        // When the stream is closed, protobuf doesn't raise an EOFException,
        // instead, it returns a null message object.
        throw new EOFException();
      }

      Status status = response.getStatus();
      if (status == Status.SUCCESS) {
        Message rpcResponseType;
        try {
          rpcResponseType = ProtobufRpcEngine.Invoker
              .getReturnProtoType(ProtobufRpcEngine.Server.getMethod(protocol,
                  param.getMethodName()));
        } catch (Exception e) {
          throw new RuntimeException(e); // local exception
        }
        Builder builder = rpcResponseType.newBuilderForType();
        builder.mergeDelimitedFrom(in);
        Message value = builder.build();

        return value;
      } else if (status == Status.ERROR) {
        RpcException exceptionResponse = RpcException.parseDelimitedFrom(in);
        RemoteException remoteException = new RemoteException(
            exceptionResponse.getExceptionName(),
            exceptionResponse.getStackTrace());
        throw remoteException.unwrapRemoteException();
      } else if (status == Status.FATAL) {
        RpcException exceptionResponse = RpcException.parseDelimitedFrom(in);
        // Close the connection
        LOG.error("Fatal Exception.", exceptionResponse);
        RemoteException remoteException = new RemoteException(
            exceptionResponse.getExceptionName(),
            exceptionResponse.getStackTrace());
        throw remoteException.unwrapRemoteException();
      } else {
        throw new IOException("What happened?");
      }
    } catch (Exception e) {
      if (e instanceof RemoteException) {
        ((RemoteException) e).unwrapRemoteException();
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Override as non-synchronized method because the method is thread safe.
   */
  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request)
      throws IOException {
    try {
      CallFuture<List<ByteBuffer>> transceiverFuture = new CallFuture<List<ByteBuffer>>();
      transceive(request, transceiverFuture);
      return transceiverFuture.get();
    } catch (InterruptedException e) {
      LOG.info("failed to get the response", e);
      throw new IOException(e);
    } catch (ExecutionException e) {
      LOG.warn("failed to get the response", e);
      throw new IOException(e);
    }
  }

  @Override
  public void transceive(List<ByteBuffer> request,
      Callback<List<ByteBuffer>> callback) throws IOException {
    stateLock.readLock().lock();
    try {
      int serial = serialGenerator.incrementAndGet();
      NettyDataPack dataPack = new NettyDataPack(serial, request);
      requests.put(serial, callback);
      writeDataPack(dataPack);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    stateLock.readLock().lock();
    try {
      writeDataPack(new NettyDataPack(serialGenerator.incrementAndGet(),
          buffers));
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Writes a NettyDataPack, reconnecting to the remote peer if necessary. NOTE:
   * The stateLock read lock *must* be acquired before calling this method.
   *
   * @param dataPack
   *          the data pack to write.
   * @throws java.io.IOException
   *           if an error occurs connecting to the remote peer.
   */
  private void writeDataPack(NettyDataPack dataPack) throws IOException {
    getChannel().write(dataPack);
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Wasp client handler for the Netty transport
   */
  class NettyClientWaspHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
      if (e instanceof ChannelStateEvent) {
        LOG.debug(e.toString());
        ChannelStateEvent cse = (ChannelStateEvent) e;
        if ((cse.getState() == ChannelState.OPEN)
            && (Boolean.FALSE.equals(cse.getValue()))) {
          // Server closed connection; disconnect client side
          LOG.debug("Remote peer " + remoteAddr + " closed connection.");
          disconnect(false, true, null);
        }
      }
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      // channel = e.getChannel();
      super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      NettyDataPack dataPack = (NettyDataPack) e.getMessage();
      Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
      if (callback == null) {
        throw new RuntimeException("Missing previous call info");
      }
      try {
        callback.handleResult(dataPack.getDatas());
      } finally {
        requests.remove(dataPack.getSerial());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      disconnect(false, true, e.getCause());
    }

  }

  /**
   * Creates threads with unique names based on a specified name prefix.
   */
  private static class NettyTransceiverThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String prefix;

    /**
     * Creates a NettyTransceiverThreadFactory that creates threads with the
     * specified name.
     * 
     * @param prefix
     *          the name prefix to use for all threads created by this
     *          ThreadFactory. A unique ID will be appended to this prefix to
     *          form the final thread name.
     */
    public NettyTransceiverThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(prefix + " " + threadId.incrementAndGet());
      return thread;
    }
  }

  /**
   * Increment this client's reference count
   * 
   */
  synchronized void incCount() {
    refCount++;
  }

  /**
   * Decrement this client's reference count
   * 
   */
  synchronized void decCount() {
    refCount--;
  }

  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount == 0;
  }

  /**
   * @return the remoteAddr
   */
  public InetSocketAddress getRemoteAddr() {
    return remoteAddr;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}