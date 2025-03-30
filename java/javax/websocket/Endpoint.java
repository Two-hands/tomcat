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


/**
 * websocket端（服务端、客户端）需要实现对一个连接的生命周期管理
 * ** 可以使用@ServerEndpoint + （@OnOpen、@OnClose、@OnError）注解配合 来代理这个接口的功能
 */
public abstract class Endpoint {


    /**
     * 当有一个websocket新连接建立时触发
     * @param session 为新websocket连接生成的session（用于保存通讯状态）
     * @param config ??
     */
    public abstract void onOpen(Session session, EndpointConfig config);


    /**
     * 当有一个websocket连接关闭时触发
     * @param session 需要被关闭连接的session
     * @param closeReason 关闭的原因
     */
    public void onClose(Session session, CloseReason closeReason) {
        // NO-OP by default
    }

    /**
     * 当有一个websocket连接处理过程中失败时触发
     * @param session 发生异常的连接的session
     * @param throwable 异常
     */
    public void onError(Session session, Throwable throwable) {
        // NO-OP by default
    }
}
