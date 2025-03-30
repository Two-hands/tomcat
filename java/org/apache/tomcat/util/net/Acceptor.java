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
import org.apache.tomcat.util.res.StringManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Acceptor<U> implements Runnable {

    private static final Log log = LogFactory.getLog(Acceptor.class);
    private static final StringManager sm = StringManager.getManager(Acceptor.class);

    private static final int INITIAL_ERROR_DELAY = 50;
    private static final int MAX_ERROR_DELAY = 1600;

    private final AbstractEndpoint<?,U> endpoint;
    private String threadName;


    //用于控制acceptor是否应该停止
    //不要使用endpoint.isRunning来控制acceptor的状态
    // 如果使用endpoint的状态，在快速切换endpoint.stop()和endpoint.start()时可能会导致acceptor在需要停止的时候仍然在运行
    private volatile boolean stopCalled = false;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    protected volatile AcceptorState state = AcceptorState.NEW;


    public Acceptor(AbstractEndpoint<?,U> endpoint) {
        this.endpoint = endpoint;
    }


    public final AcceptorState getState() {
        return state;
    }


    final void setThreadName(final String threadName) {
        this.threadName = threadName;
    }


    final String getThreadName() {
        return threadName;
    }


    @SuppressWarnings("deprecation")
    @Override
    public void run() {

        int errorDelay = 0;
        long pauseStart = 0;

        try {
            // Loop until we receive a shutdown command

            //循环接收客户端连接，当接收到停止命令时停止接收客户端连接
            while (!stopCalled) {

                //endpoint处于暂停状态时，当前acceptor线程尝试暂停
                while (endpoint.isPaused() && !stopCalled) {

                    //acceptor设置为PAUSED状态，并记录暂停起始时间
                    if (state != AcceptorState.PAUSED) {
                        pauseStart = System.nanoTime();
                        state = AcceptorState.PAUSED;
                    }

                    //暂停累积时间<1ms时，线程无需休眠，继续循环
                    //暂停累积时间[1ms,10ms]时，线程休眠1ms
                    //暂停累积事件>10ms时，线程休眠10ms
                    if ((System.nanoTime() - pauseStart) > 1_000_000) {
                        // Paused for more than 1ms
                        try {
                            if ((System.nanoTime() - pauseStart) > 10_000_000) {
                                Thread.sleep(10);
                            } else {
                                Thread.sleep(1);
                            }
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    }
                }

                //需要停止，线程终止循环，不再接收客户端连接
                if (stopCalled) {
                    break;
                }

                //设置（恢复-若之前有暂停过）acceptor状态为RUNNING
                state = AcceptorState.RUNNING;

                try {
                    //当前接收到的客户端连接数是否超过[限定最大连接数]？
                    // - 若超过，当前线程阻塞，直到已有连接释放
                    // - 若不超过，连接数加1
                    endpoint.countUpOrAwaitConnection();

                    //endpoint处于暂停状态，不再接收新客户端连接
                    if (endpoint.isPaused()) {
                        continue;
                    }

                    U socket;
                    try {
                        //阻塞等待一个新客户端连接到来
                        socket = endpoint.serverSocketAccept();
                    } catch (Exception ioe) {
                        //获取新连接失败，回滚连接数[由于上面在获取之前新增了连接数]
                        endpoint.countDownConnection();
                        if (endpoint.isRunning()) {
                            //失败后，尝试休眠当前线程，并计算下次睡眠时间[若紧接着的下次接收连接仍然失败的话]
                            errorDelay = handleExceptionWithDelay(errorDelay);
                            // re-throw
                            throw ioe;
                        } else {
                            break;
                        }
                    }

                    //接收连接成功，重置错误延迟时间
                    errorDelay = 0;


                    if (!stopCalled && !endpoint.isPaused()) {
                        log.info(String.format("******接收到新客户端连接,socketChannel[%s]", socket));
                        //尝试将socketChannel交给Selector
                        if (!endpoint.setSocketOptions(socket)) {
                            endpoint.closeSocket(socket);
                        }
                    } else {
                        //acceptor应该终止，或endpoint暂停时：关闭socket，客户端连接数回滚[减1]
                        endpoint.destroySocket(socket);
                    }
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    String msg = sm.getString("endpoint.accept.fail");
                    // APR specific.
                    // Could push this down but not sure it is worth the trouble.
                    if (t instanceof org.apache.tomcat.jni.Error) {
                        org.apache.tomcat.jni.Error e = (org.apache.tomcat.jni.Error) t;
                        if (e.getError() == 233) {
                            // Not an error on HP-UX so log as a warning
                            // so it can be filtered out on that platform
                            // See bug 50273
                            log.warn(msg, t);
                        } else {
                            log.error(msg, t);
                        }
                    } else {
                            log.error(msg, t);
                    }
                }
            }
        } finally {
            stopLatch.countDown();
        }
        state = AcceptorState.ENDED;
    }


    /**
     * Signals the Acceptor to stop, waiting at most 10 seconds for the stop to
     * complete before returning. If the stop does not complete in that time a
     * warning will be logged.
     *
     * @deprecated This method will be removed in Tomcat 10.1.x onwards.
     *             Use {@link #stop(int)} instead.
     */
    @Deprecated
    public void stop() {
        stop(10);
    }


    /**
     * Signals the Acceptor to stop, optionally waiting for that stop process
     * to complete before returning. If a wait is requested and the stop does
     * not complete in that time a warning will be logged.
     *
     * @param waitSeconds The time to wait in seconds. Use a value less than
     *                    zero for no wait.
     */
    public void stop(int waitSeconds) {
        stopCalled = true;
        if (waitSeconds > 0) {
            try {
                if (!stopLatch.await(waitSeconds, TimeUnit.SECONDS)) {
                   log.warn(sm.getString("acceptor.stop.fail", getThreadName()));
                }
            } catch (InterruptedException e) {
                log.warn(sm.getString("acceptor.stop.interrupted", getThreadName()), e);
            }
        }
    }


    /**
     * Handles exceptions where a delay is required to prevent a Thread from
     * entering a tight loop which will consume CPU and may also trigger large
     * amounts of logging. For example, this can happen if the ulimit for open
     * files is reached.
     *
     * @param currentErrorDelay The current delay being applied on failure
     * @return  The delay to apply on the next failure
     */
    protected int handleExceptionWithDelay(int currentErrorDelay) {
        // Don't delay on first exception
        if (currentErrorDelay > 0) {
            try {
                Thread.sleep(currentErrorDelay);
            } catch (InterruptedException e) {
                // Ignore
            }
        }

        // On subsequent exceptions, start the delay at 50ms, doubling the delay
        // on every subsequent exception until the delay reaches 1.6 seconds.
        if (currentErrorDelay == 0) {
            return INITIAL_ERROR_DELAY;
        } else if (currentErrorDelay < MAX_ERROR_DELAY) {
            return currentErrorDelay * 2;
        } else {
            return MAX_ERROR_DELAY;
        }
    }


    public enum AcceptorState {
        NEW, RUNNING, PAUSED, ENDED
    }
}
