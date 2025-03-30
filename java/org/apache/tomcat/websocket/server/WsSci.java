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

import org.apache.tomcat.util.compat.JreCompat;

import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.HandlesTypes;
import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.server.ServerApplicationConfig;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * <pre>
 * 处理有关WebSocket的相关类：
 *     1、 带有@ServerEndpoint注解的类：该类用于处理
 *     2、 实现ServerApplicationConfig接口的类
 *     3、 实现Endpoint抽象类的类
 * </pre>
 */
@HandlesTypes({ServerEndpoint.class, ServerApplicationConfig.class,
        Endpoint.class})
public class WsSci implements ServletContainerInitializer {

    @Override
    public void onStartup(Set<Class<?>> clazzes, ServletContext ctx)
            throws ServletException {

        //创建并初始化WebSocketContainer
        WsServerContainer sc = init(ctx, true);

        if (clazzes == null || clazzes.size() == 0) {
            return;
        }

        //实现ServerApplicationConfig接口的类
        Set<ServerApplicationConfig> serverApplicationConfigs = new HashSet<>();
        //实现Endpoint抽象类的子类
        Set<Class<? extends Endpoint>> scannedEndpointClazzes = new HashSet<>();
        //存放已发现的含有@ServerEndpoint注解的类
        Set<Class<?>> scannedPojoEndpoints = new HashSet<>();

        //将已发现的类进行分类：
        try {

            //wsPackage="javax.websocket."
            String wsPackage = ContainerProvider.class.getName();
            wsPackage = wsPackage.substring(0, wsPackage.lastIndexOf('.') + 1);

            //遍历所有已发现的类，忽略掉不满足条件的结果：
            for (Class<?> clazz : clazzes) {
                JreCompat jreCompat = JreCompat.getInstance();
                int modifiers = clazz.getModifiers();
                if (!Modifier.isPublic(modifiers) ||
                        Modifier.isAbstract(modifiers) ||
                        Modifier.isInterface(modifiers) ||
                        !jreCompat.isExported(clazz)) {
                    // Non-public, abstract, interface or not in an exported
                    // package (Java 9+) - skip it.
                    // 忽略接口、忽略抽象类、忽略非public类...??
                    continue;
                }

                //忽略Websocket原生API包（javax.websocket）下的类
                if (clazz.getName().startsWith(wsPackage)) {
                    continue;
                }

                //添加实现ServerApplicationConfig的类并实例化
                if (ServerApplicationConfig.class.isAssignableFrom(clazz)) {
                    serverApplicationConfigs.add(
                            (ServerApplicationConfig) clazz.getConstructor().newInstance());
                }

                //添加实现Endpoint的类
                if (Endpoint.class.isAssignableFrom(clazz)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends Endpoint> endpoint =
                            (Class<? extends Endpoint>) clazz;
                    scannedEndpointClazzes.add(endpoint);
                }

                //添加标注了@ServerEndpoint注解的类
                if (clazz.isAnnotationPresent(ServerEndpoint.class)) {
                    scannedPojoEndpoints.add(clazz);
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new ServletException(e);
        }

        // 过滤后满足条件的所有实现Endpoint抽象类的子类 和 含有@ServerEndpoint注解的类的结果
        Set<ServerEndpointConfig> filteredEndpointConfigs = new HashSet<>();
        Set<Class<?>> filteredPojoEndpoints = new HashSet<>();

        //尝试使用ServerApplicationConfig进行过滤：
        if (serverApplicationConfigs.isEmpty()) {
            //若没有ServerEndpointConfig，则只关注含@ServerEndpoint注解的类
            //此时所有实现了Endpoint抽象类的子类将被忽略....
            filteredPojoEndpoints.addAll(scannedPojoEndpoints);
        } else {

            //尝试通过ServerApplicationConfig进行候选websocket服务端实现类的过滤
            for (ServerApplicationConfig config : serverApplicationConfigs) {

                //过滤实现了Endpoint抽象类的子类
                Set<ServerEndpointConfig> configFilteredEndpoints = config.getEndpointConfigs(scannedEndpointClazzes);
                if (configFilteredEndpoints != null) {
                    filteredEndpointConfigs.addAll(configFilteredEndpoints);
                }

                //过滤含有@ServerEndpoint注解的类
                Set<Class<?>> configFilteredPojos = config.getAnnotatedEndpointClasses(scannedPojoEndpoints);
                if (configFilteredPojos != null) {
                    filteredPojoEndpoints.addAll(configFilteredPojos);
                }
            }
        }

        try {
            // 向WebSocketContainer添加实现Endpoint抽象类的子类
            for (ServerEndpointConfig config : filteredEndpointConfigs) {
                sc.addEndpoint(config);
            }
            // 向化WebSocketContainer添加含@ServerEndpoint注解的类
            for (Class<?> clazz : filteredPojoEndpoints) {
                sc.addEndpoint(clazz, true);
            }
        } catch (DeploymentException e) {
            throw new ServletException(e);
        }
    }


    static WsServerContainer init(ServletContext servletContext,
            boolean initBySciMechanism) {

        //创建一个新的WebSocketContainer
        WsServerContainer sc = new WsServerContainer(servletContext);

        //向ServletContext添加一个属性：值为WebSocketContainer实例
        servletContext.setAttribute(
                Constants.SERVER_CONTAINER_SERVLET_CONTEXT_ATTRIBUTE, sc);

        //向ServletContext添加一个监听器：用于销毁HttpSession
        servletContext.addListener(new WsSessionListener(sc));
        // Can't register the ContextListener again if the ContextListener is
        // calling this method
        //
        if (initBySciMechanism) {
            //向ServletContext添加一个监听器：用于销毁WebSocketContainer
            servletContext.addListener(new WsContextListener());
        }

        return sc;
    }
}
