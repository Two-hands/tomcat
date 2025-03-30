/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package javax.websocket;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * websocket的容器
 */
public interface WebSocketContainer {

    /**
     * 默认异步发送消息的超时时间
     * @return 超时时间（毫秒），若是负数，表示永不超时
     */
    long getDefaultAsyncSendTimeout();


    /**
     * 设置异步发送超时时间
     * @param timeout 超时时间（毫秒），若是负数，表示永不超时
     */
    void setAsyncSendTimeout(long timeout);

    /**
     * 客户端连接服务端，新建一个websocket连接，并生成端与端之前的会话（Session）记录通讯状态
     * @param endpoint 服务端点
     * @param path 访问路径
     * @return 会话
     */
    Session connectToServer(Object endpoint, URI path)
            throws DeploymentException, IOException;


    /**
     * 客户端连接服务端，新建一个websocket连接，并生成端与端之前的会话（Session）记录通讯状态
     * @param annotatedEndpointClass 服务端点（含@ServerEndpoint注解的类）
     * @param path 访问路径
     * @return 会话
     */
    Session connectToServer(Class<?> annotatedEndpointClass, URI path)
            throws DeploymentException, IOException;


    /**
     * 客户端连接服务端，新建一个websocket连接，并生成端与端之前的会话（Session）记录通讯状态
     * @param endpoint 服务端点
     * @param clientEndpointConfiguration 连接的配置
     * @param path 访问路径
     * @return
     */
    Session connectToServer(Endpoint endpoint,
            ClientEndpointConfig clientEndpointConfiguration, URI path)
            throws DeploymentException, IOException;


    /**
     *  客户端连接服务端，新建一个websocket连接，并生成端与端之前的会话（Session）记录通讯状态
     * @param endpoint 服务端点
     * @param clientEndpointConfiguration 连接的配置
     * @param path 访问路径
     * @return
     */
    Session connectToServer(Class<? extends Endpoint> endpoint,
            ClientEndpointConfig clientEndpointConfiguration, URI path)
            throws DeploymentException, IOException;


    /**
     * 获取默认的Session最大空闲超时时间
     * @return 超时时间（毫秒），若是负数或0，表示永不超时
     */
    long getDefaultMaxSessionIdleTimeout();


    /**
     * 设置Session默认的最大空闲超时时间
     * @param timeout 超时时间（毫秒），若是负数或0，表示永不超时
     */
    void setDefaultMaxSessionIdleTimeout(long timeout);


    /**
     * 获取默认的消息的最大字节缓存大小
     */
    int getDefaultMaxBinaryMessageBufferSize();

    /**
     * Set the default maximum buffer size for binary messages.
     * @param max The new default maximum buffer size in bytes
     */

    /**
     * 设置默认的消息的最大字节缓存大小
     */
    void setDefaultMaxBinaryMessageBufferSize(int max);

    /**
     * Get the default maximum buffer size for text messages.
     * @return The current default maximum buffer size in characters
     */

    /**
     * 获取默认最大文本消息缓存大小
     */
    int getDefaultMaxTextMessageBufferSize();

    /**
     * 设置默认最大文本消息缓存大小
     */
    void setDefaultMaxTextMessageBufferSize(int max);

    /**
     * Get the installed extensions.
     * @return The set of extensions that are supported by this WebSocket
     *         implementation.
     */
    Set<Extension> getInstalledExtensions();
}
