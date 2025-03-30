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
package org.apache.coyote;

import org.apache.tomcat.util.net.SSLHostConfig;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract the protocol implementation, including threading, etc.
 *
 * This is the main interface to be implemented by a coyote protocol.
 * Adapter is the main interface to be implemented by a coyote servlet
 * container.
 *
 * @author Remy Maucherat
 * @author Costin Manolache
 * @see Adapter
 */
public interface ProtocolHandler {

    /**
     * 从协议处理器中获取适配器
     * @return 适配器
     */
    public Adapter getAdapter();


    /**
     * 向协议处理器中添加适配器
     * @param adapter 适配器
     */
    public void setAdapter(Adapter adapter);


    /**
     * 获取线程池（用于处理请求）
     * @return 线程池
     */
    public Executor getExecutor();


    /**
     * 设置线程池（会被连接器用于处理请求）
     * @param executor 线程池
     */
    public void setExecutor(Executor executor);


    /**
     * 获取定时线程池
     * @return 定时线程池
     */
    public ScheduledExecutorService getUtilityExecutor();


    /**
     * Set the utility executor that should be used by the protocol handler.
     * @param utilityExecutor the executor
     */
    /**
     * 设置定时线程池
     * @param utilityExecutor 定时线程池
     */
    public void setUtilityExecutor(ScheduledExecutorService utilityExecutor);


    /**
     * 初始化协议处理器
     * @throws Exception 初始化过程可能抛出的异常
     */
    public void init() throws Exception;


    /**
     * 启动初始化处理器
     * @throws Exception 启动过程中可能出现的异常
     */
    public void start() throws Exception;


    /**
     * 尝试暂停协议处理器
     * @throws Exception 若协议处理器暂停失败
     */
    public void pause() throws Exception;


    /**
     * 尝试恢复协议处理器
     * @throws Exception 若协议处理器恢复失败
     */
    public void resume() throws Exception;


    /**
     * 尝试停止协议处理器
     * @throws Exception 若协议处理器停止失败
     */
    public void stop() throws Exception;


    /**
     * 尝试销毁协议处理器
     * @throws Exception 若协议处理器销毁失败
     */
    public void destroy() throws Exception;


    /**
     * 若ServerSocket的绑定操作（定义监听端口）发生在{@link #start()}方法而不是{@link #init()}方法[即bindState=BindState.BOUND_ON_START]，
     * 尝试关闭ServerSocket以避免进一步的客户端连接，但是不要进行关闭操作。
     */
    public void closeServerSocketGraceful();

    /**
     * 等待与服务端通信的客户端连接正常关闭，当所有客户端连接均已关闭 或 此方法等待waitMillis毫秒后返回
     * @param waitMillis 最大等待所有客户端连接关闭的时长
     * @return 返回方法等待所有关闭客户端连接后还剩余的时间（即：total waitMillis - consumption time）
     */
    public long awaitConnectionsClose(long waitMillis);


    /**
     * 是否需要APR本地库？
     * @return  true - 协议处理器需要APR本地库
     */
    @Deprecated
    public boolean isAprRequired();


    /**
     * 协议处理器是否支持sendFile？
     * @return true - 支持snedFile
     */
    public boolean isSendfileSupported();


    /**
     * 添加SSL配置
     * @param sslHostConfig SSL配置
     */
    public void addSslHostConfig(SSLHostConfig sslHostConfig);


    /**
     * 获取所有SSL配置
     * @return SSL配置数组
     */
    public SSLHostConfig[] findSslHostConfigs();


    /**
     * 添加一个新协议，供HTTP/1.1升级或ALPN使用
     * @param upgradeProtocol 升级协议
     */
    public void addUpgradeProtocol(UpgradeProtocol upgradeProtocol);


    /**
     * Return all configured upgrade protocols.
     * @return the protocols
     */

    /**
     * 获取所有升级协议
     * @return 升级协议数组
     */
    public UpgradeProtocol[] findUpgradeProtocols();


    /**
     * 一些协议（如：AJP）有数据包长度限制，这个配置可以用于在应用层调整使用的缓冲区
     * @return 所需的缓冲区大小，如果无关，则为-1
     */
    public default int getDesiredBufferSize() {
        return -1;
    }


    /**
     * 默认是使用IP地址和端口号组合作为连接的唯一标识。可是某些连接不适用此方式，可以使用此这个方式去替代
     * @return 连接的唯一标识
     */
    public default String getId() {
        return null;
    }


    /**
     * 通过给定的协议创建具体的协议处理器
     * @param protocol 协议名称
     * @param apr 是否使用arp协议？
     * @return 协议处理器
     */
    @SuppressWarnings("deprecation")
    public static ProtocolHandler create(String protocol, boolean apr)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

        if (protocol == null || "HTTP/1.1".equals(protocol)
                || (!apr && org.apache.coyote.http11.Http11NioProtocol.class.getName().equals(protocol))
                || (apr && org.apache.coyote.http11.Http11AprProtocol.class.getName().equals(protocol))) {
            if (apr) {
                return new org.apache.coyote.http11.Http11AprProtocol();
            } else {
                //大多数情况都是使用HTTP/1.1协议处理器
                return new org.apache.coyote.http11.Http11NioProtocol();
            }
        } else if ("AJP/1.3".equals(protocol)
                || (!apr && org.apache.coyote.ajp.AjpNioProtocol.class.getName().equals(protocol))
                || (apr && org.apache.coyote.ajp.AjpAprProtocol.class.getName().equals(protocol))) {
            if (apr) {
                return new org.apache.coyote.ajp.AjpAprProtocol();
            } else {
                return new org.apache.coyote.ajp.AjpNioProtocol();
            }
        } else {
            // Instantiate protocol handler
            Class<?> clazz = Class.forName(protocol);
            return (ProtocolHandler) clazz.getConstructor().newInstance();
        }
    }


}
