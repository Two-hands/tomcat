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
package org.apache.tomcat.websocket.server;

import org.apache.coyote.http11.upgrade.UpgradeInfo;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.net.AbstractEndpoint.Handler.SocketState;
import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.apache.tomcat.util.res.StringManager;
import org.apache.tomcat.websocket.Transformation;
import org.apache.tomcat.websocket.WsFrameBase;
import org.apache.tomcat.websocket.WsIOException;
import org.apache.tomcat.websocket.WsSession;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class WsFrameServer extends WsFrameBase {

    private final Log log = LogFactory.getLog(WsFrameServer.class); // must not be static
    private static final StringManager sm = StringManager.getManager(WsFrameServer.class);

    private final SocketWrapperBase<?> socketWrapper;
    private final UpgradeInfo upgradeInfo;
    private final ClassLoader applicationClassLoader;


    public WsFrameServer(SocketWrapperBase<?> socketWrapper, UpgradeInfo upgradeInfo, WsSession wsSession,
            Transformation transformation, ClassLoader applicationClassLoader) {
        super(wsSession, transformation);
        this.socketWrapper = socketWrapper;
        this.upgradeInfo = upgradeInfo;
        this.applicationClassLoader = applicationClassLoader;
    }


    /**
     *  当socket中输入流有数据时（客户端的请求数据），进行读取客户端请求数据....
     */
    private void onDataAvailable() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("wsFrameServer.onDataAvailable");
        }


        if (isOpen() && inputBuffer.hasRemaining() && !isSuspended()) {
            //当前inputBuffer可能还有为读的数据（由于read操作被挂起）
            //继续处理剩余的数据
            processInputBuffer();
        }

        //循环读取和处理数据
        while (isOpen() && !isSuspended()) {
            inputBuffer.mark();
            inputBuffer.position(inputBuffer.limit()).limit(inputBuffer.capacity());

            //1、循环从socket接收缓冲区中读取数据到inputBuffer缓冲区中
            int read = socketWrapper.read(false, inputBuffer);
            inputBuffer.limit(inputBuffer.position()).reset();
            if (read < 0) {
                //读取错误
                throw new EOFException();
            } else if (read == 0) {
                //无数据，直接返回
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("wsFrameServer.bytesRead", Integer.toString(read)));
            }

            //2、处理数据
            processInputBuffer();
        }
    }


    @Override
    protected void updateStats(long payloadLength) {
        upgradeInfo.addMsgsReceived(1);
        upgradeInfo.addBytesReceived(payloadLength);
    }


    @Override
    protected boolean isMasked() {
        // Data is from the client so it should be masked
        return true;
    }


    @Override
    protected Transformation getTransformation() {
        // Overridden to make it visible to other classes in this package
        return super.getTransformation();
    }


    @Override
    protected boolean isOpen() {
        // Overridden to make it visible to other classes in this package
        return super.isOpen();
    }


    @Override
    protected Log getLog() {
        return log;
    }


    @Override
    protected void sendMessageText(boolean last) throws WsIOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(applicationClassLoader);
            super.sendMessageText(last);
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }


    @Override
    protected void sendMessageBinary(ByteBuffer msg, boolean last) throws WsIOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(applicationClassLoader);
            super.sendMessageBinary(msg, last);
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }


    @Override
    protected void resumeProcessing() {
        socketWrapper.processSocket(SocketEvent.OPEN_READ, true);
    }


    SocketState notifyDataAvailable() throws IOException {
        while (isOpen()) {
            switch (getReadState()) {
            case WAITING:
                //将"等待"状态改为"处理"状态后再处理数据
                if (!changeReadState(ReadState.WAITING, ReadState.PROCESSING)) {
                    continue;
                }
                try {
                    //读取客户端发送的数据
                    return doOnDataAvailable();
                } catch (IOException e) {
                    changeReadState(ReadState.CLOSING);
                    throw e;
                }
            case SUSPENDING_WAIT:
                if (!changeReadState(ReadState.SUSPENDING_WAIT, ReadState.SUSPENDED)) {
                    continue;
                }
                return SocketState.SUSPENDED;
            default:
                throw new IllegalStateException(
                        sm.getString("wsFrameServer.illegalReadState", getReadState()));
            }
        }

        return SocketState.CLOSED;
    }


    private SocketState doOnDataAvailable() throws IOException {

        //处理客户端发送的数据
        onDataAvailable();


        while (isOpen()) {
            switch (getReadState()) {
            case PROCESSING:
                if (!changeReadState(ReadState.PROCESSING, ReadState.WAITING)) {
                    continue;
                }
                return SocketState.UPGRADED;
            case SUSPENDING_PROCESS:
                if (!changeReadState(ReadState.SUSPENDING_PROCESS, ReadState.SUSPENDED)) {
                    continue;
                }
                return SocketState.SUSPENDED;
            default:
                throw new IllegalStateException(
                        sm.getString("wsFrameServer.illegalReadState", getReadState()));
            }
        }

        return SocketState.CLOSED;
    }
}
