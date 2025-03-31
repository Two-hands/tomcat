/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.tomcat.util.net;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.collections.SynchronizedQueue;
import org.apache.tomcat.util.collections.SynchronizedStack;
import org.apache.tomcat.util.compat.JreCompat;
import org.apache.tomcat.util.compat.JrePlatform;
import org.apache.tomcat.util.net.AbstractEndpoint.Handler.SocketState;
import org.apache.tomcat.util.net.Acceptor.AcceptorState;
import org.apache.tomcat.util.net.jsse.JSSESupport;

import javax.net.ssl.SSLEngine;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <pre>
 *  NIO专有线程，提供以下服务：
 *   1、线程Acceptor：接收客户端新SocketChannel连接【阻塞式等待新客户端连接】
 *   2、线程Poller：含有Selector，感知SocketChannel注册的事件【非阻塞/阻塞两种方式等待读、写等事件的发生】，以事件驱动模式用单个线程高效管理多个连接的读写。
 *   3、提供线程池真正处理SocketChannel的读写请求（由Poller感知后，添加到线程池） 【当有事件发生时，将其交于线程池并发处理】
 *
 *
 *  * Acceptor（线程）是AbstractEndpoint成员变量，由AbstractEndpoint创建并启动，持有Endpoint引用
 *  * NioEndpoint.Poller（线程）是NioEndpoint的成员变量，也是其内部类（可以与其外部类NioEndpoint通信），由NioEndpoint创建并启动。
 *
 *  **  1、Acceptor不断轮询 NioEndpoint.serverSock（ServerSocketChannel）监听客户端的连接
 *  **  2、将接收到的新连接交于NioEndpoint.Poller注册感兴趣的"读事件"，当SocketChannel发生"读"事件时，NioEndpoint.Poller会将其交予NioEndpoint.executor并发处理
 *
 *             监听连接                                   注册Socket
 *    Acceptor ——————> NioEndpoint.serverSock（接收Socket） ————>  NioEndpoint.poller
 *
 *                       监听Socket请求(读事件)
 *    NioEndpoint.poller  ——————————> NioEndpoint.executor（并发处理Socket请求）
 *
 *
 * </pre>
 */
public class NioEndpoint extends AbstractJsseEndpoint<NioChannel,SocketChannel> {


    // -------------------------------------------------------------- Constants


    private static final Log log = LogFactory.getLog(NioEndpoint.class);
    private static final Log logHandshake = LogFactory.getLog(NioEndpoint.class.getName() + ".handshake");


    public static final int OP_REGISTER = 0x100; //register interest op

    // ----------------------------------------------------------------- Fields

    //服务端套接字通道（ServerSocketChannel），专门用于监听客户端的套接字通道（SocketChannel）连接
    private volatile ServerSocketChannel serverSock = null;

    //注册SocketChannel到Selector，负责感知新事件
    private Poller poller = null;


    @Override
    public void bind() throws Exception {
        //创建ServerSocketChannel对象，绑定服务监听端口...
        initServerSocket();

        setStopLatch(new CountDownLatch(1));

        // Initialize SSL if needed
        initialiseSsl();
    }

    // Separated out to make it easier for folks that extend NioEndpoint to
    // implement custom [server]sockets
    protected void initServerSocket() throws Exception {
        if (getUseInheritedChannel()) {
            // Retrieve the channel provided by the OS
            Channel ic = System.inheritedChannel();
            if (ic instanceof ServerSocketChannel) {
                serverSock = (ServerSocketChannel) ic;
            }
            if (serverSock == null) {
                throw new IllegalArgumentException(sm.getString("endpoint.init.bind.inherited"));
            }
        } else if (getUnixDomainSocketPath() != null) {
            SocketAddress sa = JreCompat.getInstance().getUnixDomainSocketAddress(getUnixDomainSocketPath());
            serverSock = JreCompat.getInstance().openUnixDomainServerSocketChannel();
            serverSock.bind(sa, getAcceptCount());
            if (getUnixDomainSocketPathPermissions() != null) {
                Path path = Paths.get(getUnixDomainSocketPath());
                Set<PosixFilePermission> permissions =
                    PosixFilePermissions.fromString(getUnixDomainSocketPathPermissions());
                if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                    FileAttribute<Set<PosixFilePermission>> attrs = PosixFilePermissions.asFileAttribute(permissions);
                    Files.setAttribute(path, attrs.name(), attrs.value());
                } else {
                    java.io.File file = path.toFile();
                    if (permissions.contains(PosixFilePermission.OTHERS_READ) && !file.setReadable(true, false)) {
                        log.warn(sm.getString("endpoint.nio.perms.readFail", file.getPath()));
                    }
                    if (permissions.contains(PosixFilePermission.OTHERS_WRITE) && !file.setWritable(true, false)) {
                        log.warn(sm.getString("endpoint.nio.perms.writeFail", file.getPath()));
                    }
                }
            }
        } else {
            serverSock = ServerSocketChannel.open();
            socketProperties.setProperties(serverSock.socket());
            InetSocketAddress addr = new InetSocketAddress(getAddress(), getPortWithOffset());
            serverSock.bind(addr, getAcceptCount());
        }
        serverSock.configureBlocking(true); //mimic APR behavior
    }


    /**
     * Stop latch used to wait for poller stop
     */
    private volatile CountDownLatch stopLatch = null;
    private SynchronizedStack<PollerEvent> eventCache;
    private SynchronizedStack<NioChannel> nioChannels;
    private SocketAddress previousAcceptedSocketRemoteAddress = null;
    private long previousAcceptedSocketNanoTime = 0;
    private boolean useInheritedChannel = false;
    private String unixDomainSocketPath = null;

    private String unixDomainSocketPathPermissions = null;
    private int pollerThreadPriority = Thread.NORM_PRIORITY;
    private long selectorTimeout = 1000;

    public void setUseInheritedChannel(boolean useInheritedChannel) { this.useInheritedChannel = useInheritedChannel; }

    public boolean getUseInheritedChannel() { return useInheritedChannel; }

    public String getUnixDomainSocketPath() { return this.unixDomainSocketPath; }

    public void setUnixDomainSocketPath(String unixDomainSocketPath) {
        this.unixDomainSocketPath = unixDomainSocketPath;
    }

    public String getUnixDomainSocketPathPermissions() { return this.unixDomainSocketPathPermissions; }

    public void setUnixDomainSocketPathPermissions(String unixDomainSocketPathPermissions) {
        this.unixDomainSocketPathPermissions = unixDomainSocketPathPermissions;
    }

    public void setPollerThreadPriority(int pollerThreadPriority) { this.pollerThreadPriority = pollerThreadPriority; }

    public int getPollerThreadPriority() { return pollerThreadPriority; }

    public void setSelectorTimeout(long timeout) { this.selectorTimeout = timeout;}

    public long getSelectorTimeout() { return this.selectorTimeout; }


    /**
     * Is deferAccept supported?
     */
    @Override
    public boolean getDeferAccept() {
        // Not supported
        return false;
    }



    public int getKeepAliveCount() {
        if (poller == null) {
            return 0;
        } else {
            return poller.getKeyCount();
        }
    }


    @Override
    public String getId() {
        if (getUseInheritedChannel()) {
            return "JVMInheritedChannel";
        } else if (getUnixDomainSocketPath() != null) {
            return getUnixDomainSocketPath();
        } else {
            return null;
        }
    }



    /**
     * Start the NIO endpoint, creating acceptor, poller threads.
     */
    @Override
    public void startInternal() throws Exception {

        if (!running) {
            running = true;
            paused = false;

            if (socketProperties.getProcessorCache() != 0) {
                processorCache = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE,
                        socketProperties.getProcessorCache());
            }
            if (socketProperties.getEventCache() != 0) {
                eventCache = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE,
                        socketProperties.getEventCache());
            }
            if (socketProperties.getBufferPool() != 0) {
                nioChannels = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE,
                        socketProperties.getBufferPool());
            }

            // Create worker collection
            if (getExecutor() == null) {
                createExecutor();
            }

            initializeConnectionLatch();

            // Start poller thread
            poller = new Poller();
            Thread pollerThread = new Thread(poller, getName() + "-Poller");
            pollerThread.setPriority(threadPriority);
            pollerThread.setDaemon(true);
            pollerThread.start();

            startAcceptorThread();
        }
    }


    /**
     * Stop the endpoint. This will cause all processing threads to stop.
     */
    @Override
    public void stopInternal() {
        if (!paused) {
            pause();
        }
        if (running) {
            running = false;
            acceptor.stop(10);
            if (poller != null) {
                poller.destroy();
                poller = null;
            }
            try {
                if (!getStopLatch().await(selectorTimeout + 100, TimeUnit.MILLISECONDS)) {
                    log.warn(sm.getString("endpoint.nio.stopLatchAwaitFail"));
                }
            } catch (InterruptedException e) {
                log.warn(sm.getString("endpoint.nio.stopLatchAwaitInterrupted"), e);
            }
            shutdownExecutor();
            if (eventCache != null) {
                eventCache.clear();
                eventCache = null;
            }
            if (nioChannels != null) {
                NioChannel socket;
                while ((socket = nioChannels.pop()) != null) {
                    socket.free();
                }
                nioChannels = null;
            }
            if (processorCache != null) {
                processorCache.clear();
                processorCache = null;
            }
        }
    }


    /**
     * Deallocate NIO memory pools, and close server socket.
     */
    @Override
    public void unbind() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Destroy initiated for " +
                    new InetSocketAddress(getAddress(),getPortWithOffset()));
        }
        if (running) {
            stop();
        }
        try {
            doCloseServerSocket();
        } catch (IOException ioe) {
            getLog().warn(sm.getString("endpoint.serverSocket.closeFailed", getName()), ioe);
        }
        destroySsl();
        super.unbind();
        if (getHandler() != null ) {
            getHandler().recycle();
        }
        if (log.isDebugEnabled()) {
            log.debug("Destroy completed for " +
                    new InetSocketAddress(getAddress(), getPortWithOffset()));
        }
    }


    @Override
    protected void doCloseServerSocket() throws IOException {
        try {
            if (!getUseInheritedChannel() && serverSock != null) {
                // Close server socket
                serverSock.close();
            }
            serverSock = null;
        } finally {
            if (getUnixDomainSocketPath() != null && getBindState().wasBound()) {
                Files.delete(Paths.get(getUnixDomainSocketPath()));
            }
        }
    }


    // ------------------------------------------------------ Protected Methods


    @Override
    protected void unlockAccept() {
        if (getUnixDomainSocketPath() == null) {
            super.unlockAccept();
        } else {
            // Only try to unlock the acceptor if it is necessary
            if (acceptor == null || acceptor.getState() != AcceptorState.RUNNING) {
                return;
            }
            try {
                SocketAddress sa = JreCompat.getInstance().getUnixDomainSocketAddress(getUnixDomainSocketPath());
                try (SocketChannel socket = JreCompat.getInstance().openUnixDomainSocketChannel()) {
                    // With a UDS, expect no delay connecting and no defer accept
                    socket.connect(sa);
                }
                // Wait for upto 1000ms acceptor threads to unlock
                long waitLeft = 1000;
                while (waitLeft > 0 &&
                        acceptor.getState() == AcceptorState.RUNNING) {
                    Thread.sleep(5);
                    waitLeft -= 5;
                }
            } catch(Throwable t) {
                ExceptionUtils.handleThrowable(t);
                if (getLog().isDebugEnabled()) {
                    getLog().debug(sm.getString(
                            "endpoint.debug.unlock.fail", String.valueOf(getPortWithOffset())), t);
                }
            }
        }
    }


    protected SynchronizedStack<NioChannel> getNioChannels() {
        return nioChannels;
    }


    protected Poller getPoller() {
        return poller;
    }


    protected CountDownLatch getStopLatch() {
        return stopLatch;
    }


    protected void setStopLatch(CountDownLatch stopLatch) {
        this.stopLatch = stopLatch;
    }


    /**
     * Process the specified connection.
     * @param socket The socket channel
     * @return <code>true</code> if the socket was correctly configured
     *  and processing may continue, <code>false</code> if the socket needs to be
     *  close immediately
     */
    @Override
    protected boolean setSocketOptions(SocketChannel socket) {
        NioSocketWrapper socketWrapper = null;
        try {

            //尝试从缓存池中获取NioChannel，若获取失败[池中无空余资源]则创建
            NioChannel channel = null;
            if (nioChannels != null) {
                channel = nioChannels.pop();
            }
            if (channel == null) {
                SocketBufferHandler bufhandler = new SocketBufferHandler(
                        socketProperties.getAppReadBufSize(),
                        socketProperties.getAppWriteBufSize(),
                        socketProperties.getDirectBuffer());
                if (isSSLEnabled()) {
                    channel = new SecureNioChannel(bufhandler, this);
                } else {
                    channel = new NioChannel(bufhandler);
                }
            }

            //创建NioSocketWrapper
            NioSocketWrapper newWrapper = new NioSocketWrapper(channel, this);
            channel.reset(socket, newWrapper);

            //缓存当前socketChannel和与其关联的NioSocketWrapper
            connections.put(socket, newWrapper);
            socketWrapper = newWrapper;


            //设置SocketChannel为非阻塞模式（才可以向Selector注册）
            socket.configureBlocking(false);
            if (getUnixDomainSocketPath() == null) {
                //配置socket，如socket的读写缓冲大小，是否keep-alive，读时时间...
                socketProperties.setProperties(socket.socket());
            }

            //根据soTimeout值设置socket的读、写超时时间
            socketWrapper.setReadTimeout(getConnectionTimeout());
            socketWrapper.setWriteTimeout(getConnectionTimeout());
            //设置http请求的最大keep-alive数[一个连接可以连续处理多少次请求数据]
            socketWrapper.setKeepAliveLeft(NioEndpoint.this.getMaxKeepAliveRequests());

            //注册socket到selector上
            poller.register(socketWrapper);
            return true;
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            try {
                log.error(sm.getString("endpoint.socketOptionsError"), t);
            } catch (Throwable tt) {
                ExceptionUtils.handleThrowable(tt);
            }
            if (socketWrapper == null) {
                destroySocket(socket);
            }
        }
        // Tell to close the socket if needed
        return false;
    }


    @Override
    protected void destroySocket(SocketChannel socket) {
        countDownConnection();
        try {
            socket.close();
        } catch (IOException ioe) {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.err.close"), ioe);
            }
        }
    }


    @Override
    protected NetworkChannel getServerSocket() {
        return serverSock;
    }


    @Override
    protected SocketChannel serverSocketAccept() throws Exception {
        SocketChannel result = serverSock.accept();

        // Bug does not affect Windows. Skip the check on that platform.
        if (!JrePlatform.IS_WINDOWS) {
            //非windows系统在[极短时间内，即100ns内]可能会连续接收同一个socket
            SocketAddress currentRemoteAddress = result.getRemoteAddress();
            long currentNanoTime = System.nanoTime();
            if (currentRemoteAddress.equals(previousAcceptedSocketRemoteAddress) &&
                    currentNanoTime - previousAcceptedSocketNanoTime < 1000) {
                throw new IOException(sm.getString("endpoint.err.duplicateAccept"));
            }
            previousAcceptedSocketRemoteAddress = currentRemoteAddress;
            previousAcceptedSocketNanoTime = currentNanoTime;
        }

        return result;
    }


    @Override
    protected Log getLog() {
        return log;
    }


    @Override
    protected SocketProcessorBase<NioChannel> createSocketProcessor(
            SocketWrapperBase<NioChannel> socketWrapper, SocketEvent event) {
        return new SocketProcessor(socketWrapper, event);
    }

    // ----------------------------------------------------- Poller Inner Classes

    /**
     * PollerEvent, cacheable object for poller events to avoid GC
     */
    public static class PollerEvent {

        private NioSocketWrapper socketWrapper;
        private int interestOps;

        public PollerEvent(NioSocketWrapper socketWrapper, int intOps) {
            reset(socketWrapper, intOps);
        }

        public void reset(NioSocketWrapper socketWrapper, int intOps) {
            this.socketWrapper = socketWrapper;
            interestOps = intOps;
        }

        public NioSocketWrapper getSocketWrapper() {
            return socketWrapper;
        }

        public int getInterestOps() {
            return interestOps;
        }

        public void reset() {
            reset(null, 0);
        }

        @Override
        public String toString() {
            return "Poller event: socket [" + socketWrapper.getSocket() + "], socketWrapper [" + socketWrapper +
                    "], interestOps [" + interestOps + "]";
        }
    }

    /**
     * Poller class.
     */
    public class Poller implements Runnable {

        private Selector selector;
        private final SynchronizedQueue<PollerEvent> events =
                new SynchronizedQueue<>();

        private volatile boolean close = false;
        // Optimize expiration handling
        private long nextExpiration = 0;

        private AtomicLong wakeupCounter = new AtomicLong(0);

        private volatile int keyCount = 0;

        public Poller() throws IOException {
            this.selector = Selector.open();
        }

        public int getKeyCount() { return keyCount; }

        public Selector getSelector() { return selector; }

        /**
         * Destroy the poller.
         */
        protected void destroy() {
            // Wait for polltime before doing anything, so that the poller threads
            // exit, otherwise parallel closure of sockets which are still
            // in the poller can cause problems
            close = true;
            selector.wakeup();
        }

        private void addEvent(PollerEvent event) {
            events.offer(event);
            if (wakeupCounter.incrementAndGet() == 0) {
                selector.wakeup();
            }
        }

        private PollerEvent createPollerEvent(NioSocketWrapper socketWrapper, int interestOps) {
            PollerEvent r = null;
            if (eventCache != null) {
                r = eventCache.pop();
            }
            if (r == null) {
                r = new PollerEvent(socketWrapper, interestOps);
            } else {
                r.reset(socketWrapper, interestOps);
            }
            return r;
        }

        /**
         * Add specified socket and associated pool to the poller. The socket will
         * be added to a temporary array, and polled first after a maximum amount
         * of time equal to pollTime (in most cases, latency will be much lower,
         * however).
         *
         * @param socketWrapper to add to the poller
         * @param interestOps Operations for which to register this socket with
         *                    the Poller
         */
        public void add(NioSocketWrapper socketWrapper, int interestOps) {
            PollerEvent pollerEvent = createPollerEvent(socketWrapper, interestOps);
            addEvent(pollerEvent);
            if (close) {
                processSocket(socketWrapper, SocketEvent.STOP, false);
            }
        }

        /**
         * Processes events in the event queue of the Poller.
         *
         * @return <code>true</code> if some events were processed,
         *   <code>false</code> if queue was empty
         */
        public boolean events() {
            boolean result = false;

            PollerEvent pe;
            for (int i = 0, size = events.size(); i < size && (pe = events.poll()) != null; i++ ) {
                result = true;
                NioSocketWrapper socketWrapper = pe.getSocketWrapper();
                SocketChannel sc = socketWrapper.getSocket().getIOChannel();
                int interestOps = pe.getInterestOps();
                if (sc == null) {
                    log.warn(sm.getString("endpoint.nio.nullSocketChannel"));
                    /*
                    关闭SocketWrapper：
                        1、回收当前socketWrapper关联的processor，并放入recycledProcessors池中
                        2、客户端连接计数减1
                        3、从connections中移除关联的socket
                        4、从nioChannels中回收关联的NioChannel
                     */
                    socketWrapper.close();
                } else if (interestOps == OP_REGISTER) {
                    try {
                        //注册新客户端socket到Selector中，并关注读事件
                        sc.register(getSelector(), SelectionKey.OP_READ, socketWrapper);
                    } catch (Exception x) {
                        log.error(sm.getString("endpoint.nio.registerFail"), x);
                    }
                } else {
                    //获取当前socket在Selector上注册的SelectionKey
                    final SelectionKey key = sc.keyFor(getSelector());
                    if (key == null) {
                        //事件已被取消（可能由于关闭原因）和socket在处理前从Selector中移除了
                        //在这时要对连接计数减一，因为socket关闭时不会减少连接数。
                        socketWrapper.close();
                    } else {
                        final NioSocketWrapper attachment = (NioSocketWrapper) key.attachment();
                        if (attachment != null) {
                            try {
                                //该socket已经注册到selector，可能有新事件需要注册
                                int ops = key.interestOps() | interestOps;
                                attachment.interestOps(ops);
                                key.interestOps(ops);
                            } catch (CancelledKeyException ckx) {
                                //取消事件注册，关闭socket连接
                                cancelledKey(key, socketWrapper);
                            }
                        } else {
                            //取消事件注册，关闭socket连接
                            cancelledKey(key, socketWrapper);
                        }
                    }
                }


                if (running && eventCache != null) {
                    pe.reset();
                    eventCache.push(pe);
                }
            }

            return result;
        }


        /**
         * Acceptor线程将新客户端连接包装成事件，放入队列中[线程间通讯媒介]，
         * 后面由Poller线程处理事件[将新socket以读事件注册到Selector]
         * @param socketWrapper socket包装
         */
        public void register(final NioSocketWrapper socketWrapper) {
            socketWrapper.interestOps(SelectionKey.OP_READ);
            //包装成事件，尝试注册到selector
            PollerEvent pollerEvent = createPollerEvent(socketWrapper, OP_REGISTER);
            //添加事件，交由Poller线程自己注册socket，而不是当前线程[Acceptor]注册
            addEvent(pollerEvent);
        }

        /**
         * 取消事件注册，关闭socket连接
         * @param sk 事件注册key
         * @param socketWrapper socket包装
         */
        public void cancelledKey(SelectionKey sk, SocketWrapperBase<NioChannel> socketWrapper) {

            if (JreCompat.isJre11Available() && socketWrapper != null) {
                socketWrapper.close();
            } else {
                log.info(String.format("******%s-> SelectionKey[%s],%s-> socketChannel[%s]",
                    null == sk ? "" : (sk.isValid() ? "取消KEY" : ""),
                    null == sk ? "null" : sk,
                    null == socketWrapper ? "" : "关闭通道",
                    null == socketWrapper ? "null" : socketWrapper.getSocket().sc));

                try {
                    // If is important to cancel the key first, otherwise a deadlock may occur between the
                    // poller select and the socket channel close which would cancel the key
                    // This workaround is not needed on Java 11+
                    if (sk != null) {
                        sk.attach(null);
                        if (sk.isValid()) {
                            sk.cancel();
                        }
                    }
                } catch (Throwable e) {
                    ExceptionUtils.handleThrowable(e);
                    if (log.isDebugEnabled()) {
                        log.error(sm.getString("endpoint.debug.channelCloseFail"), e);
                    }
                } finally {
                    if (socketWrapper != null) {
                        socketWrapper.close();
                    }
                }
            }
        }



        @Override
        public void run() {

            //循环处理：
            // 1、尝试从队列中获取事件，注册新socketChannel到Selector
            // 2、从Selector中获取已经触发的感兴趣的读、写事件并处理
            while (true) {

                boolean hasEvents = false;
                try {
                    if (!close) {
                        //处理队列中的事件，注册新socketChannel到Selector中
                        hasEvents = events();
                        if (wakeupCounter.getAndSet(-1) > 0) {
                            // If we are here, means we have other stuff to do.
                            // Do a non-blocking select
                            keyCount = selector.selectNow();
                        } else {
                            keyCount = selector.select(selectorTimeout);
                        }
                        wakeupCounter.set(0);
                    }


                    if (close) {
                        events();
                        timeout(0, false);
                        try {
                            selector.close();
                        } catch (IOException ioe) {
                            log.error(sm.getString("endpoint.nio.selectorCloseFail"), ioe);
                        }
                        break;
                    }

                    // Either we timed out or we woke up, process events first
                    if (keyCount == 0) {
                        hasEvents = (hasEvents | events());
                    }
                } catch (Throwable x) {
                    ExceptionUtils.handleThrowable(x);
                    log.error(sm.getString("endpoint.nio.selectorLoopError"), x);
                    continue;
                }

                Iterator<SelectionKey> iterator =
                    keyCount > 0 ? selector.selectedKeys().iterator() : null;

                //处理selector上触发的事件
                while (iterator != null && iterator.hasNext()) {
                    SelectionKey sk = iterator.next();
                    iterator.remove();
                    NioSocketWrapper socketWrapper = (NioSocketWrapper) sk.attachment();

                    if (socketWrapper != null) {
                        log.info(String.format("******发生「%s」事件,socketChannel[%s]",
                            sk.readyOps() == SelectionKey.OP_READ ? "READ" : (sk.readyOps() == SelectionKey.OP_WRITE)
                                ? "WRITE" : (sk.readyOps() == SelectionKey.OP_CONNECT) ? "CONNECT" : "ACCEPT",
                            socketWrapper.getSocket().sc));
                        //处理触发注册事件的socket
                        processKey(sk, socketWrapper);
                    }
                }

                //关闭 读写超时的socket
                timeout(keyCount,hasEvents);
            }

            getStopLatch().countDown();
        }

        protected void processKey(SelectionKey sk, NioSocketWrapper socketWrapper) {
            try {
                if (close) {
                    cancelledKey(sk, socketWrapper);
                } else if (sk.isValid()) {
                    if (sk.isReadable() || sk.isWritable()) {
                        if (socketWrapper.getSendfileData() != null) {
                            //处理文件下载...
                            processSendfile(sk, socketWrapper, false);
                        } else {
                            //从Selector中注销当前套接字的事件，避免多个线程同时处理一个socket的同一种事件....
                            //sk.readyOps() - 获取当前socket已就绪的事件
                            unreg(sk, socketWrapper, sk.readyOps());
                            boolean closeSocket = false;
                            // Read goes before write
                            if (sk.isReadable()) {
                                if (socketWrapper.readOperation != null) {
                                    if (!socketWrapper.readOperation.process()) {
                                        closeSocket = true;
                                    }
                                } else if (socketWrapper.readBlocking) {
                                    synchronized (socketWrapper.readLock) {
                                        socketWrapper.readBlocking = false;
                                        socketWrapper.readLock.notify();
                                    }
                                }
                                //处理读事件
                                else if (!processSocket(socketWrapper, SocketEvent.OPEN_READ, true)) {
                                    //处理socket读事件失败
                                    closeSocket = true;
                                }
                            }
                            if (!closeSocket && sk.isWritable()) {
                                if (socketWrapper.writeOperation != null) {
                                    if (!socketWrapper.writeOperation.process()) {
                                        closeSocket = true;
                                    }
                                } else if (socketWrapper.writeBlocking) {
                                    synchronized (socketWrapper.writeLock) {
                                        socketWrapper.writeBlocking = false;
                                        socketWrapper.writeLock.notify();
                                    }
                                }
                                //处理写事件
                                else if (!processSocket(socketWrapper, SocketEvent.OPEN_WRITE, true)) {
                                    //处理socket写事件失败
                                    closeSocket = true;
                                }
                            }
                            if (closeSocket) {
                                cancelledKey(sk, socketWrapper);
                            }
                        }
                    }
                } else {
                    // Invalid key
                    cancelledKey(sk, socketWrapper);
                }
            } catch (CancelledKeyException ckx) {
                cancelledKey(sk, socketWrapper);
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
                log.error(sm.getString("endpoint.nio.keyProcessingError"), t);
            }
        }

        public SendfileState processSendfile(SelectionKey sk, NioSocketWrapper socketWrapper,
                boolean calledByProcessor) {
            NioChannel sc = null;
            try {
                unreg(sk, socketWrapper, sk.readyOps());
                SendfileData sd = socketWrapper.getSendfileData();

                if (log.isTraceEnabled()) {
                    log.trace("Processing send file for: " + sd.fileName);
                }

                if (sd.fchannel == null) {
                    // Setup the file channel
                    File f = new File(sd.fileName);
                    @SuppressWarnings("resource") // Closed when channel is closed
                    FileInputStream fis = new FileInputStream(f);
                    sd.fchannel = fis.getChannel();
                }

                // Configure output channel
                sc = socketWrapper.getSocket();
                // TLS/SSL channel is slightly different
                WritableByteChannel wc = ((sc instanceof SecureNioChannel) ? sc : sc.getIOChannel());

                // We still have data in the buffer
                if (sc.getOutboundRemaining() > 0) {
                    if (sc.flushOutbound()) {
                        socketWrapper.updateLastWrite();
                    }
                } else {
                    long written = sd.fchannel.transferTo(sd.pos, sd.length, wc);
                    if (written > 0) {
                        sd.pos += written;
                        sd.length -= written;
                        socketWrapper.updateLastWrite();
                    } else {
                        // Unusual not to be able to transfer any bytes
                        // Check the length was set correctly
                        if (sd.fchannel.size() <= sd.pos) {
                            throw new IOException(sm.getString("endpoint.sendfile.tooMuchData"));
                        }
                    }
                }
                if (sd.length <= 0 && sc.getOutboundRemaining()<=0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send file complete for: " + sd.fileName);
                    }
                    socketWrapper.setSendfileData(null);
                    try {
                        sd.fchannel.close();
                    } catch (Exception ignore) {
                    }
                    // For calls from outside the Poller, the caller is
                    // responsible for registering the socket for the
                    // appropriate event(s) if sendfile completes.
                    if (!calledByProcessor) {
                        switch (sd.keepAliveState) {
                        case NONE: {
                            if (log.isDebugEnabled()) {
                                log.debug("Send file connection is being closed");
                            }
                            poller.cancelledKey(sk, socketWrapper);
                            break;
                        }
                        case PIPELINED: {
                            if (log.isDebugEnabled()) {
                                log.debug("Connection is keep alive, processing pipe-lined data");
                            }
                            if (!processSocket(socketWrapper, SocketEvent.OPEN_READ, true)) {
                                poller.cancelledKey(sk, socketWrapper);
                            }
                            break;
                        }
                        case OPEN: {
                            if (log.isDebugEnabled()) {
                                log.debug("Connection is keep alive, registering back for OP_READ");
                            }
                            reg(sk, socketWrapper, SelectionKey.OP_READ);
                            break;
                        }
                        }
                    }
                    return SendfileState.DONE;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("OP_WRITE for sendfile: " + sd.fileName);
                    }
                    if (calledByProcessor) {
                        add(socketWrapper, SelectionKey.OP_WRITE);
                    } else {
                        reg(sk, socketWrapper, SelectionKey.OP_WRITE);
                    }
                    return SendfileState.PENDING;
                }
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to complete sendfile request:", e);
                }
                if (!calledByProcessor && sc != null) {
                    poller.cancelledKey(sk, socketWrapper);
                }
                return SendfileState.ERROR;
            } catch (Throwable t) {
                log.error(sm.getString("endpoint.sendfile.error"), t);
                if (!calledByProcessor && sc != null) {
                    poller.cancelledKey(sk, socketWrapper);
                }
                return SendfileState.ERROR;
            }
        }

        protected void unreg(SelectionKey sk, NioSocketWrapper socketWrapper, int readyOps) {
            // 感知到socket（当前处理的套接字）的触发事件，开始处理前，先从Selector中注销socket感兴趣的事件；
            // 避免多个线程干扰同一个套接字（后续由线程池线程处理处理套接字，若不注销，会出现一个线程在处理当前
            // 套接字时，又有事件发生后会有其他线程处理，导致套接字被多个线程处理....）

            //readyOps - 当前socket已就绪的事件
            //~readyOps - 按位取反，如：SelectionKey.OP_READ = 1（0000 0001），取反后值为-2（1111 1110）
            // sk.interestOps() & (~readyOps) - 注销当前触发的事件
            reg(sk, socketWrapper, sk.interestOps() & (~readyOps));
        }

        protected void reg(SelectionKey sk, NioSocketWrapper socketWrapper, int intops) {
            sk.interestOps(intops);
            socketWrapper.interestOps(intops);
        }

        protected void timeout(int keyCount, boolean hasEvents) {
            long now = System.currentTimeMillis();
            // This method is called on every loop of the Poller. Don't process
            // timeouts on every loop of the Poller since that would create too
            // much load and timeouts can afford to wait a few seconds.
            // However, do process timeouts if any of the following are true:
            // - the selector simply timed out (suggests there isn't much load)
            // - the nextExpiration time has passed
            // - the server socket is being closed
            if (nextExpiration > 0 &&
                (keyCount > 0 || hasEvents) &&
                (now < nextExpiration) &&
                !close) {
                return;
            }
            int keycount = 0;
            try {
                //遍历Selector上所有注册的key
                for (SelectionKey key : selector.keys()) {
                    keycount++;
                    NioSocketWrapper socketWrapper = (NioSocketWrapper) key.attachment();
                    try {
                        if (socketWrapper == null) {
                            //key没有关联socket，不进行处理，取消key
                            cancelledKey(key, null);
                        } else if (close) {
                            //Poller关闭，取消所有key
                            key.interestOps(0);
                            // Avoid duplicate stop calls
                            socketWrapper.interestOps(0);
                            cancelledKey(key, socketWrapper);
                        } else if (socketWrapper.interestOpsHas(SelectionKey.OP_READ) ||
                                  socketWrapper.interestOpsHas(SelectionKey.OP_WRITE)) {
                            //有注册读写事件


                            //是否读超时？
                            boolean readTimeout = false;
                            //是否写超时？
                            boolean writeTimeout = false;
                            if (socketWrapper.interestOpsHas(SelectionKey.OP_READ)) {
                                //有注册读事件，获取socket最近读的时间，判断是否超时
                                long delta = now - socketWrapper.getLastRead();
                                long timeout = socketWrapper.getReadTimeout();
                                if (timeout > 0 && delta > timeout) {
                                    readTimeout = true;
                                }
                            }

                            if (!readTimeout && socketWrapper.interestOpsHas(SelectionKey.OP_WRITE)) {
                                //有注册写事件，获取socket最近写的时间，判断是否超时
                                long delta = now - socketWrapper.getLastWrite();
                                long timeout = socketWrapper.getWriteTimeout();
                                if (timeout > 0 && delta > timeout) {
                                    writeTimeout = true;
                                }
                            }

                            if (readTimeout || writeTimeout) {
                                //socket有读、写超时

                                //不再注册任何事件
                                key.interestOps(0);
                                socketWrapper.interestOps(0);
                                socketWrapper.setError(new SocketTimeoutException());
                                if (readTimeout && socketWrapper.readOperation != null) {
                                    if (!socketWrapper.readOperation.process()) {
                                        cancelledKey(key, socketWrapper);
                                    }
                                } else if (writeTimeout && socketWrapper.writeOperation != null) {
                                    if (!socketWrapper.writeOperation.process()) {
                                        cancelledKey(key, socketWrapper);
                                    }
                                }
                                //socket出现读、写超时：处理当前socket[以Error事件]
                                else if (!processSocket(socketWrapper, SocketEvent.ERROR, true)) {
                                    //处理socket，发布ERROR事件，处理失败后取消key，关闭socket
                                    log.info(String.format("******读写超时，ERROR事件处理：SelectionKey[%s],socketChannel[%s]",key,socketWrapper.getSocket().sc));
                                    cancelledKey(key, socketWrapper);
                                }
                            }
                        }
                    } catch (CancelledKeyException ckx) {
                        cancelledKey(key, socketWrapper);
                    }
                }
            } catch (ConcurrentModificationException cme) {
                // See https://bz.apache.org/bugzilla/show_bug.cgi?id=57943
                log.warn(sm.getString("endpoint.nio.timeoutCme"), cme);
            }
            // For logging purposes only
            long prevExp = nextExpiration;
            nextExpiration = System.currentTimeMillis() +
                    socketProperties.getTimeoutInterval();
            if (log.isTraceEnabled()) {
                log.trace("timeout completed: keys processed=" + keycount +
                        "; now=" + now + "; nextExpiration=" + prevExp +
                        "; keyCount=" + keyCount + "; hasEvents=" + hasEvents +
                        "; eval=" + ((now < prevExp) && (keyCount>0 || hasEvents) && (!close) ));
            }

        }
    }

    // --------------------------------------------------- Socket Wrapper Class

    public static class NioSocketWrapper extends SocketWrapperBase<NioChannel> {

        private final SynchronizedStack<NioChannel> nioChannels;
        private final Poller poller;

        private int interestOps = 0;
        private volatile SendfileData sendfileData = null;
        private volatile long lastRead = System.currentTimeMillis();
        private volatile long lastWrite = lastRead;

        private final Object readLock;

        //从socket中读取数据是否需要阻塞？true - 需要阻塞
        private volatile boolean readBlocking = false;
        private final Object writeLock;
        private volatile boolean writeBlocking = false;

        public NioSocketWrapper(NioChannel channel, NioEndpoint endpoint) {
            super(channel, endpoint);
            if (endpoint.getUnixDomainSocketPath() != null) {
                // Pretend localhost for easy compatibility
                localAddr = "127.0.0.1";
                localName = "localhost";
                localPort = 0;
                remoteAddr = "127.0.0.1";
                remoteHost = "localhost";
                remotePort = 0;
            }
            nioChannels = endpoint.getNioChannels();
            poller = endpoint.getPoller();
            socketBufferHandler = channel.getBufHandler();
            readLock = (readPending == null) ? new Object() : readPending;
            writeLock = (writePending == null) ? new Object() : writePending;
        }

        public Poller getPoller() { return poller; }
        public int interestOps() { return interestOps; }
        public int interestOps(int ops) { this.interestOps  = ops; return ops; }
        public boolean interestOpsHas(int targetOp) {
            return (this.interestOps() & targetOp) == targetOp;
        }

        public void setSendfileData(SendfileData sf) { this.sendfileData = sf;}
        public SendfileData getSendfileData() { return this.sendfileData; }

        public void updateLastWrite() { lastWrite = System.currentTimeMillis(); }
        public long getLastWrite() { return lastWrite; }
        public void updateLastRead() { lastRead = System.currentTimeMillis(); }
        public long getLastRead() { return lastRead; }

        @Override
        public boolean isReadyForRead() throws IOException {
            socketBufferHandler.configureReadBufferForRead();

            if (socketBufferHandler.getReadBuffer().remaining() > 0) {
                return true;
            }

            fillReadBuffer(false);

            boolean isReady = socketBufferHandler.getReadBuffer().position() > 0;
            return isReady;
        }


        @Override
        public int read(boolean block, byte[] b, int off, int len) throws IOException {
            int nRead = populateReadBuffer(b, off, len);
            if (nRead > 0) {
                return nRead;
                /*
                 * Since more bytes may have arrived since the buffer was last
                 * filled, it is an option at this point to perform a
                 * non-blocking read. However correctly handling the case if
                 * that read returns end of stream adds complexity. Therefore,
                 * at the moment, the preference is for simplicity.
                 */
            }

            // Fill the read buffer as best we can.
            nRead = fillReadBuffer(block);
            updateLastRead();

            // Fill as much of the remaining byte array as possible with the
            // data that was just read
            if (nRead > 0) {
                socketBufferHandler.configureReadBufferForRead();
                nRead = Math.min(nRead, len);
                socketBufferHandler.getReadBuffer().get(b, off, nRead);
            }
            return nRead;
        }


        @Override
        public int read(boolean block, ByteBuffer to) throws IOException {

            //** 1、临时缓冲区有数据，直接从临时缓冲区中获取数据到to中
            int nRead = populateReadBuffer(to);
            if (nRead > 0) {
                return nRead;
            }

            //** 2、临时缓冲区无数据，需从socket接受缓冲区中读取数据
            //每次从socket接收缓冲区中读取尽量多的数据（减少内核态、用户态的频繁切换带来的消耗）：
            //  目标缓冲区(to)容量与socketBufferHandler.getReadBuffer()容器相比
            //    1、若前者容量更大，直接将socket接收缓冲区的数据写入
            //    2、若后者容量更大，将socket接收缓冲区的数据写入，可供目标缓冲区(to)多次读取
            int limit = socketBufferHandler.getReadBuffer().capacity();
            if (to.remaining() >= limit) {
                //当前缓冲（to）空间足够
                to.limit(to.position() + limit);
                //尝试从socket通道中读取数据到to中
                nRead = fillReadBuffer(block, to);
                if (log.isDebugEnabled()) {
                    log.debug("Socket: [" + this + "], Read direct from socket: [" + nRead + "]");
                }
                //更新最近一次读取时间...
                updateLastRead();
            } else {
                //若当前缓冲（to）空间不足，将数据尽可能多的从socket通道中读取并放入socketBufferHandler.getReadBuffer()中
                nRead = fillReadBuffer(block);
                if (log.isDebugEnabled()) {
                    log.debug("Socket: [" + this + "], Read into buffer: [" + nRead + "]");
                }
                updateLastRead();

                //尽可能多的将socketBufferHandler.getReadBuffer()中读取到的数据转移到to缓存中
                if (nRead > 0) {
                    nRead = populateReadBuffer(to);
                }
            }
            return nRead;
        }


        @Override
        protected void doClose() {
            if (log.isDebugEnabled()) {
                log.debug("Calling [" + getEndpoint() + "].closeSocket([" + this + "])");
            }
            try {
                getEndpoint().connections.remove(getSocket().getIOChannel());
                if (getSocket().isOpen()) {
                    getSocket().close(true);
                }
                if (getEndpoint().running) {
                    if (nioChannels == null || !nioChannels.push(getSocket())) {
                        getSocket().free();
                    }
                }
            } catch (Throwable e) {
                ExceptionUtils.handleThrowable(e);
                if (log.isDebugEnabled()) {
                    log.error(sm.getString("endpoint.debug.channelCloseFail"), e);
                }
            } finally {
                socketBufferHandler = SocketBufferHandler.EMPTY;
                nonBlockingWriteBuffer.clear();
                reset(NioChannel.CLOSED_NIO_CHANNEL);
            }
            try {
                SendfileData data = getSendfileData();
                if (data != null && data.fchannel != null && data.fchannel.isOpen()) {
                    data.fchannel.close();
                }
            } catch (Throwable e) {
                ExceptionUtils.handleThrowable(e);
                if (log.isDebugEnabled()) {
                    log.error(sm.getString("endpoint.sendfile.closeError"), e);
                }
            }
        }

        private int fillReadBuffer(boolean block) throws IOException {
            socketBufferHandler.configureReadBufferForWrite();
            return fillReadBuffer(block, socketBufferHandler.getReadBuffer());
        }


        /**
         * 尝试调用socketChannel#read方法获取客户端输入的数据，并将读取到的数据填充到buffer缓存中
         * 若block=true，表示当通道没有可读数据时，线程需要阻塞等待（限时等待/不指定时间等待）直到读取到数据或读取超时
         * 若block=false，表示尝试从通道中读取一次可读数据，无论是否有数据到来
         * @param block  是否阻塞？ true - 需要阻塞（限时等待/"无限期"等待）
         * @param buffer 将读取到的数据缓存到buffer中
         * @return 读取到的字节数（大于0时表示具体读取到的字节数，为0时表示没有数据）
         */
        private int fillReadBuffer(boolean block, ByteBuffer buffer) throws IOException {
            int n = 0;
            if (getSocket() == NioChannel.CLOSED_NIO_CHANNEL) {
                throw new ClosedChannelException();
            }

            // block - 阻塞等待，当没有可读数据，线程阻塞等待，直到读取到数据或等待超时...
            // !block - 直接调用socketChannel.read非阻塞读取数据（无论是否读取到数据）
            if (block) {
                //阻塞式读取：
                //timeout - socketReadTimeout
                long timeout = getReadTimeout();
                long startNanos = 0;

                do {

                    if (startNanos > 0) {
                        //读取线程发生过阻塞（等待数据）
                        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        if (elapsedMillis == 0) {
                            elapsedMillis = 1;
                        }
                        timeout -= elapsedMillis;

                        //等待超时...
                        if (timeout <= 0) {
                            throw new SocketTimeoutException();
                        }
                    }

                    //尝试从SocketChannel中读取数据，并放入到buffer中
                    n = getSocket().read(buffer);
                    if (n == -1) {
                        //读取发生错误：网络问题？？？
                        throw new EOFException();
                    } else if (n == 0) {
                        //没有读取到数据（SocketChannel设置为非阻塞...）
                        //标志当前读取需要阻塞，并向Selector注册读事件，等待数据的到来...
                        if (!readBlocking) {
                            readBlocking = true;
                            registerReadInterest();
                        }

                        synchronized (readLock) {
                            if (readBlocking) {
                                //限时阻塞当前线程等待数据的到来
                                try {
                                    if (timeout > 0) {
                                        //限时等待
                                        startNanos = System.nanoTime();
                                        readLock.wait(timeout);
                                    } else {
                                        //无限时等待
                                        readLock.wait();
                                    }
                                } catch (InterruptedException e) {
                                    // Continue
                                }
                            }
                        }
                    }
                } while (n == 0);
            } else {
                //非阻塞式读取
                n = getSocket().read(buffer);
                if (n == -1) {
                    throw new EOFException();
                }
            }
            return n;
        }


        @Override
        protected boolean flushNonBlocking() throws IOException {
            boolean dataLeft = socketOrNetworkBufferHasDataLeft();

            // Write to the socket, if there is anything to write
            if (dataLeft) {
                doWrite(false);
                dataLeft = socketOrNetworkBufferHasDataLeft();
            }

            if (!dataLeft && !nonBlockingWriteBuffer.isEmpty()) {
                dataLeft = nonBlockingWriteBuffer.write(this, false);

                if (!dataLeft && socketOrNetworkBufferHasDataLeft()) {
                    doWrite(false);
                    dataLeft = socketOrNetworkBufferHasDataLeft();
                }
            }

            return dataLeft;
        }


        /*
         * https://bz.apache.org/bugzilla/show_bug.cgi?id=66076
         *
         * When using TLS an additional buffer is used for the encrypted data
         * before it is written to the network. It is possible for this network
         * output buffer to contain data while the socket write buffer is empty.
         *
         * For NIO with non-blocking I/O, this case is handling by ensuring that
         * flush only returns false (i.e. no data left to flush) if all buffers
         * are empty.
         */
        private boolean socketOrNetworkBufferHasDataLeft() {
            return !socketBufferHandler.isWriteBufferEmpty() || getSocket().getOutboundRemaining() > 0;
        }


        @Override
        protected void doWrite(boolean block, ByteBuffer buffer) throws IOException {
            int n = 0;
            if (getSocket() == NioChannel.CLOSED_NIO_CHANNEL) {
                throw new ClosedChannelException();
            }
            if (block) {
                if (previousIOException != null) {
                    /*
                     * Socket has previously timed out.
                     *
                     * Blocking writes assume that buffer is always fully
                     * written so there is no code checking for incomplete
                     * writes, retaining the unwritten data and attempting to
                     * write it as part of a subsequent write call.
                     *
                     * Because of the above, when a timeout is triggered we need
                     * to skip subsequent attempts to write as otherwise it will
                     * appear to the client as if some data was dropped just
                     * before the connection is lost. It is better if the client
                     * just sees the dropped connection.
                     */
                    throw new IOException(previousIOException);
                }
                long timeout = getWriteTimeout();
                long startNanos = 0;
                do {
                    if (startNanos > 0) {
                        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        if (elapsedMillis == 0) {
                            elapsedMillis = 1;
                        }
                        timeout -= elapsedMillis;
                        if (timeout <= 0) {
                            previousIOException = new SocketTimeoutException();
                            throw previousIOException;
                        }
                    }
                    n = getSocket().write(buffer);
                    if (n == -1) {
                        throw new EOFException();
                    } else if (n == 0 && (buffer.hasRemaining() || getSocket().getOutboundRemaining() > 0)) {
                        // n == 0 could be an incomplete write but it could also
                        // indicate that a previous incomplete write of the
                        // outbound buffer (for TLS) has now completed. Only
                        // block if there is still data to write.
                        writeBlocking = true;
                        registerWriteInterest();
                        synchronized (writeLock) {
                            if (writeBlocking) {
                                try {
                                    if (timeout > 0) {
                                        startNanos = System.nanoTime();
                                        writeLock.wait(timeout);
                                    } else {
                                        writeLock.wait();
                                    }
                                } catch (InterruptedException e) {
                                    // Continue
                                }
                                writeBlocking = false;
                            }
                        }
                    } else if (startNanos > 0) {
                        // If something was written, reset timeout
                        timeout = getWriteTimeout();
                        startNanos = 0;
                    }
                } while (buffer.hasRemaining() || getSocket().getOutboundRemaining() > 0);
            } else {
                do {
                    n = getSocket().write(buffer);
                    if (n == -1) {
                        throw new EOFException();
                    }
                } while (n > 0 && buffer.hasRemaining());
                // If there is data left in the buffer the socket will be registered for
                // write further up the stack. This is to ensure the socket is only
                // registered for write once as both container and user code can trigger
                // write registration.
            }
            updateLastWrite();
        }


        @Override
        public void registerReadInterest() {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.debug.registerRead", this));
            }
            getPoller().add(this, SelectionKey.OP_READ);
        }


        @Override
        public void registerWriteInterest() {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.debug.registerWrite", this));
            }
            getPoller().add(this, SelectionKey.OP_WRITE);
        }


        @Override
        public SendfileDataBase createSendfileData(String filename, long pos, long length) {
            return new SendfileData(filename, pos, length);
        }


        @Override
        public SendfileState processSendfile(SendfileDataBase sendfileData) {
            setSendfileData((SendfileData) sendfileData);
            SelectionKey key = getSocket().getIOChannel().keyFor(getPoller().getSelector());
            // Might as well do the first write on this thread
            return getPoller().processSendfile(key, this, true);
        }


        @Override
        protected void populateRemoteAddr() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                InetAddress inetAddr = sc.socket().getInetAddress();
                if (inetAddr != null) {
                    remoteAddr = inetAddr.getHostAddress();
                }
            }
        }


        @Override
        protected void populateRemoteHost() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                InetAddress inetAddr = sc.socket().getInetAddress();
                if (inetAddr != null) {
                    remoteHost = inetAddr.getHostName();
                    if (remoteAddr == null) {
                        remoteAddr = inetAddr.getHostAddress();
                    }
                }
            }
        }


        @Override
        protected void populateRemotePort() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                remotePort = sc.socket().getPort();
            }
        }


        @Override
        protected void populateLocalName() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                InetAddress inetAddr = sc.socket().getLocalAddress();
                if (inetAddr != null) {
                    localName = inetAddr.getHostName();
                }
            }
        }


        @Override
        protected void populateLocalAddr() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                InetAddress inetAddr = sc.socket().getLocalAddress();
                if (inetAddr != null) {
                    localAddr = inetAddr.getHostAddress();
                }
            }
        }


        @Override
        protected void populateLocalPort() {
            SocketChannel sc = getSocket().getIOChannel();
            if (sc != null) {
                localPort = sc.socket().getLocalPort();
            }
        }


        @Override
        public SSLSupport getSslSupport() {
            if (getSocket() instanceof SecureNioChannel) {
                SecureNioChannel ch = (SecureNioChannel) getSocket();
                return ch.getSSLSupport();
            }
            return null;
        }


        @Override
        public void doClientAuth(SSLSupport sslSupport) throws IOException {
            SecureNioChannel sslChannel = (SecureNioChannel) getSocket();
            SSLEngine engine = sslChannel.getSslEngine();
            if (!engine.getNeedClientAuth()) {
                // Need to re-negotiate SSL connection
                engine.setNeedClientAuth(true);
                sslChannel.rehandshake(getEndpoint().getConnectionTimeout());
                ((JSSESupport) sslSupport).setSession(engine.getSession());
            }
        }


        @Override
        public void setAppReadBufHandler(ApplicationBufferHandler handler) {
            getSocket().setAppReadBufHandler(handler);
        }

        @Override
        protected <A> OperationState<A> newOperationState(boolean read,
                ByteBuffer[] buffers, int offset, int length,
                BlockingMode block, long timeout, TimeUnit unit, A attachment,
                CompletionCheck check, CompletionHandler<Long, ? super A> handler,
                Semaphore semaphore, VectoredIOCompletionHandler<A> completion) {
            return new NioOperationState<>(read, buffers, offset, length, block,
                    timeout, unit, attachment, check, handler, semaphore, completion);
        }

        private class NioOperationState<A> extends OperationState<A> {
            private volatile boolean inline = true;
            private NioOperationState(boolean read, ByteBuffer[] buffers, int offset, int length,
                    BlockingMode block, long timeout, TimeUnit unit, A attachment, CompletionCheck check,
                    CompletionHandler<Long, ? super A> handler, Semaphore semaphore,
                    VectoredIOCompletionHandler<A> completion) {
                super(read, buffers, offset, length, block,
                        timeout, unit, attachment, check, handler, semaphore, completion);
            }

            @Override
            protected boolean isInline() {
                return inline;
            }

            @Override
            protected boolean hasOutboundRemaining() {
                return getSocket().getOutboundRemaining() > 0;
            }

            @Override
            public void run() {
                // Perform the IO operation
                // Called from the poller to continue the IO operation
                long nBytes = 0;
                if (getError() == null) {
                    try {
                        synchronized (this) {
                            if (!completionDone) {
                                // This filters out same notification until processing
                                // of the current one is done
                                if (log.isDebugEnabled()) {
                                    log.debug("Skip concurrent " + (read ? "read" : "write") + " notification");
                                }
                                return;
                            }
                            if (read) {
                                // Read from main buffer first
                                if (!socketBufferHandler.isReadBufferEmpty()) {
                                    // There is still data inside the main read buffer, it needs to be read first
                                    socketBufferHandler.configureReadBufferForRead();
                                    for (int i = 0; i < length && !socketBufferHandler.isReadBufferEmpty(); i++) {
                                        nBytes += transfer(socketBufferHandler.getReadBuffer(), buffers[offset + i]);
                                    }
                                }
                                if (nBytes == 0) {
                                    nBytes = getSocket().read(buffers, offset, length);
                                    updateLastRead();
                                }
                            } else {
                                boolean doWrite = true;
                                // Write from main buffer first
                                if (socketOrNetworkBufferHasDataLeft()) {
                                    // There is still data inside the main write buffer, it needs to be written first
                                    socketBufferHandler.configureWriteBufferForRead();
                                    do {
                                        nBytes = getSocket().write(socketBufferHandler.getWriteBuffer());
                                    } while (socketOrNetworkBufferHasDataLeft() && nBytes > 0);
                                    if (socketOrNetworkBufferHasDataLeft()) {
                                        doWrite = false;
                                    }
                                    // Preserve a negative value since it is an error
                                    if (nBytes > 0) {
                                        nBytes = 0;
                                    }
                                }
                                if (doWrite) {
                                    long n = 0;
                                    do {
                                        n = getSocket().write(buffers, offset, length);
                                        if (n == -1) {
                                            nBytes = n;
                                        } else {
                                            nBytes += n;
                                        }
                                    } while (n > 0);
                                    updateLastWrite();
                                }
                            }
                            if (nBytes != 0 || (!buffersArrayHasRemaining(buffers, offset, length) &&
                                    (read || !socketOrNetworkBufferHasDataLeft()))) {
                                completionDone = false;
                            }
                        }
                    } catch (IOException e) {
                        setError(e);
                    }
                }
                if (nBytes > 0 || (nBytes == 0 && !buffersArrayHasRemaining(buffers, offset, length) &&
                        (read || !socketOrNetworkBufferHasDataLeft()))) {
                    // The bytes processed are only updated in the completion handler
                    completion.completed(Long.valueOf(nBytes), this);
                } else if (nBytes < 0 || getError() != null) {
                    IOException error = getError();
                    if (error == null) {
                        error = new EOFException();
                    }
                    completion.failed(error, this);
                } else {
                    // As soon as the operation uses the poller, it is no longer inline
                    inline = false;
                    if (read) {
                        registerReadInterest();
                    } else {
                        registerWriteInterest();
                    }
                }
            }
        }
    }


    // ---------------------------------------------- SocketProcessor Inner Class

    /**
     * This class is the equivalent of the Worker, but will simply use in an
     * external Executor thread pool.
     */
    protected class SocketProcessor extends SocketProcessorBase<NioChannel> {

        public SocketProcessor(SocketWrapperBase<NioChannel> socketWrapper, SocketEvent event) {
            super(socketWrapper, event);
        }

        @Override
        protected void doRun() {
            /*
             * Do not cache and re-use the value of socketWrapper.getSocket() in
             * this method. If the socket closes the value will be updated to
             * CLOSED_NIO_CHANNEL and the previous value potentially re-used for
             * a new connection. That can result in a stale cached value which
             * in turn can result in unintentionally closing currently active
             * connections.
             */
            Poller poller = NioEndpoint.this.poller;
            if (poller == null) {
                socketWrapper.close();
                return;
            }

            try {
                int handshake;
                try {
                    //当socketWrapper为NioSocketWrapper时，此时始终为true
                    if (socketWrapper.getSocket().isHandshakeComplete()) {
                        // No TLS handshaking required. Let the handler
                        // process this socket / event combination.
                        handshake = 0;
                    } else if (event == SocketEvent.STOP || event == SocketEvent.DISCONNECT ||
                            event == SocketEvent.ERROR) {
                        // Unable to complete the TLS handshake. Treat it as
                        // if the handshake failed.
                        handshake = -1;
                    } else {
                        ///当socketWrapper为NioSocketWrapper时，handshake始终为0
                        handshake = socketWrapper.getSocket().handshake(event == SocketEvent.OPEN_READ, event == SocketEvent.OPEN_WRITE);
                        // The handshake process reads/writes from/to the
                        // socket. status may therefore be OPEN_WRITE once
                        // the handshake completes. However, the handshake
                        // happens when the socket is opened so the status
                        // must always be OPEN_READ after it completes. It
                        // is OK to always set this as it is only used if
                        // the handshake completes.
                        event = SocketEvent.OPEN_READ;
                    }
                } catch (IOException x) {
                    handshake = -1;
                    if (logHandshake.isDebugEnabled()) {
                        logHandshake.debug(sm.getString("endpoint.err.handshake",
                                socketWrapper.getRemoteAddr(), Integer.toString(socketWrapper.getRemotePort())), x);
                    }
                } catch (CancelledKeyException ckx) {
                    handshake = -1;
                }

                //握手成功
                if (handshake == 0) {
                    SocketState state;
                    // 开始处理Socket的请求
                    //getHandler -> AbstractProtocol#ConnectionHandler（含ProtocolHandler）
                    if (event == null) {
                        state = getHandler().process(socketWrapper, SocketEvent.OPEN_READ);
                    } else {
                        state = getHandler().process(socketWrapper, event);
                    }

                    //getHandler().process(socketWrapper,event)方法返回CLOSED状态情况：
                    //  1、socketWrapper为null
                    //  2、event为DISCONNECT，或ERROR
                    //  3、请求解析处理失败，或ProtocolHandler暂停
                    //  4、当前请求已经处理完成，关闭当前socket连接
                    if (state == SocketState.CLOSED) {
                        //取消key，关闭socket
                        poller.cancelledKey(getSelectionKey(), socketWrapper);
                    }
                } else if (handshake == -1 ) {
                    getHandler().process(socketWrapper, SocketEvent.CONNECT_FAIL);
                    poller.cancelledKey(getSelectionKey(), socketWrapper);
                } else if (handshake == SelectionKey.OP_READ){
                    socketWrapper.registerReadInterest();
                } else if (handshake == SelectionKey.OP_WRITE){
                    socketWrapper.registerWriteInterest();
                }
            } catch (CancelledKeyException cx) {
                poller.cancelledKey(getSelectionKey(), socketWrapper);
            } catch (VirtualMachineError vme) {
                ExceptionUtils.handleThrowable(vme);
            } catch (Throwable t) {
                log.error(sm.getString("endpoint.processing.fail"), t);
                poller.cancelledKey(getSelectionKey(), socketWrapper);
            } finally {
                socketWrapper = null;
                event = null;
                //return to cache
                if (running && processorCache != null) {
                    processorCache.push(this);
                }
            }
        }

        private SelectionKey getSelectionKey() {
            // Shortcut for Java 11 onwards
            if (JreCompat.isJre11Available()) {
                return null;
            }

            SocketChannel socketChannel = socketWrapper.getSocket().getIOChannel();
            if (socketChannel == null) {
                return null;
            }

            return socketChannel.keyFor(NioEndpoint.this.poller.getSelector());
        }
    }


    // ----------------------------------------------- SendfileData Inner Class

    /**
     * SendfileData class.
     */
    public static class SendfileData extends SendfileDataBase {

        public SendfileData(String filename, long pos, long length) {
            super(filename, pos, length);
        }

        protected volatile FileChannel fchannel;
    }
}
