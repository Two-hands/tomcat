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
package javax.websocket.server;

import javax.websocket.Endpoint;
import java.util.Set;

/**
 * 用来过滤已发现的websocket服务端的实现类，并尝试将其解析并封装为ServerEndpointConfig返回
 * 此类的实现将通过ServletContainerInitializer扫描发现
 */
public interface ServerApplicationConfig {

    /**
     * 对Endpoint抽象类的实现类（websocket服务端实现）进行过滤，只返回满足条件的结果
     * 对需要返回的Endpoint子类构建其ServerEndpointConfig
     * @param scanned 已发现的实现Endpoint抽象类的子类...
     */
    Set<ServerEndpointConfig> getEndpointConfigs(
            Set<Class<? extends Endpoint>> scanned);


    /**
     * <pre>
     * 对含有@ServerEndpoint注解的类进行过滤，值返回满足条件的结果
     *    ** 对于使用@ServerEndpoint注解来标注websocket服务端的类来讲，
     *       它的ServerEndpointConfig是DefaultServerEndpointConfig
     * </pre>
     * @param scanned 已发现的含有@ServerEndpoint注解的类
     */
    Set<Class<?>> getAnnotatedEndpointClasses(Set<Class<?>> scanned);
}
