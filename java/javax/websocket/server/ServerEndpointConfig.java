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

import javax.websocket.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;


/**
 * websocket服务端使用的配置
 * 可以自定义实现这个接口，也可以使用Builder构建出系统提供的默认实现（DefaultServerEndpointConfig）
 */
public interface ServerEndpointConfig extends EndpointConfig {

    /**
     * 获取websocket服务端具体实现逻辑的类
     */
    Class<?> getEndpointClass();

    /**
     * 返回此WebSocket服务器端点注册的路径
     */
    String getPath();

    List<String> getSubprotocols();

    List<Extension> getExtensions();

    Configurator getConfigurator();


    /**
     * 快速构建ServerEndpointConfig（默认实现类：DefaultServerEndpointConfig）
     *  默认实现类的endpointClass、path不能为null，其他字段可选
     */
    final class Builder {

        public static Builder create(
                Class<?> endpointClass, String path) {
            return new Builder(endpointClass, path);
        }


        //不能为null
        private final Class<?> endpointClass;

        //不能为null，且必须以'/'开头
        private final String path;

        //可选
        private List<Class<? extends Encoder>> encoders =
                Collections.emptyList();

        //可选
        private List<Class<? extends Decoder>> decoders =
                Collections.emptyList();

        //可选
        private List<String> subprotocols = Collections.emptyList();

        //可选
        private List<Extension> extensions = Collections.emptyList();

        //可选
        private Configurator configurator =
                Configurator.fetchContainerDefaultConfigurator();


        private Builder(Class<?> endpointClass,
                String path) {
            if (endpointClass == null) {
                throw new IllegalArgumentException("Endpoint class may not be null");
            }
            if (path == null) {
                throw new IllegalArgumentException("Path may not be null");
            }
            if (path.isEmpty()) {
                throw new IllegalArgumentException("Path may not be empty");
            }
            if (path.charAt(0) != '/') {
                throw new IllegalArgumentException("Path must start with '/'");
            }
            this.endpointClass = endpointClass;
            this.path = path;
        }

        public ServerEndpointConfig build() {
            return new DefaultServerEndpointConfig(endpointClass, path,
                subprotocols, extensions, encoders, decoders, configurator);
        }


        public Builder encoders(
                List<Class<? extends Encoder>> encoders) {
            if (encoders == null || encoders.size() == 0) {
                this.encoders = Collections.emptyList();
            } else {
                this.encoders = Collections.unmodifiableList(encoders);
            }
            return this;
        }


        public Builder decoders(
                List<Class<? extends Decoder>> decoders) {
            if (decoders == null || decoders.size() == 0) {
                this.decoders = Collections.emptyList();
            } else {
                this.decoders = Collections.unmodifiableList(decoders);
            }
            return this;
        }


        public Builder subprotocols(
                List<String> subprotocols) {
            if (subprotocols == null || subprotocols.size() == 0) {
                this.subprotocols = Collections.emptyList();
            } else {
                this.subprotocols = Collections.unmodifiableList(subprotocols);
            }
            return this;
        }


        public Builder extensions(
                List<Extension> extensions) {
            if (extensions == null || extensions.size() == 0) {
                this.extensions = Collections.emptyList();
            } else {
                this.extensions = Collections.unmodifiableList(extensions);
            }
            return this;
        }


        public Builder configurator(Configurator serverEndpointConfigurator) {
            if (serverEndpointConfigurator == null) {
                this.configurator = Configurator.fetchContainerDefaultConfigurator();
            } else {
                this.configurator = serverEndpointConfigurator;
            }
            return this;
        }
    }


    /**
     * websocket服务端连接客户端的协议相关功能的拓展：
     *    具体的Configurator要么从/META-INF/services/Configurator文件中获取，
     *    要么使用默认实现（DefaultServerEndpointConfigurator）
     */
    class Configurator {

        /**
         * 具体的Configurator实例，单例模式
         */
        private static volatile Configurator defaultImpl = null;
        private static final Object defaultImplLock = new Object();

        private static final String DEFAULT_IMPL_CLASSNAME =
                "org.apache.tomcat.websocket.server.DefaultServerEndpointConfigurator";

        static Configurator fetchContainerDefaultConfigurator() {
            if (defaultImpl == null) {
                synchronized (defaultImplLock) {
                    if (defaultImpl == null) {
                        if (System.getSecurityManager() == null) {
                            defaultImpl = loadDefault();
                        } else {
                            defaultImpl =
                                    AccessController.doPrivileged(new PrivilegedLoadDefault());
                        }
                    }
                }
            }
            return defaultImpl;
        }


        private static Configurator loadDefault() {
            Configurator result = null;

            //从META-INF/services中加载Configurator实现类
            ServiceLoader<Configurator> serviceLoader =
                    ServiceLoader.load(Configurator.class);

            Iterator<Configurator> iter = serviceLoader.iterator();
            while (result == null && iter.hasNext()) {
                result = iter.next();
            }

            // 如果没有找到，使用默认实现（DefaultServerEndpointConfigurator）
            if (result == null) {
                try {
                    @SuppressWarnings("unchecked")
                    Class<Configurator> clazz =
                            (Class<Configurator>) Class.forName(
                                    DEFAULT_IMPL_CLASSNAME);
                    result = clazz.getConstructor().newInstance();
                } catch (ReflectiveOperationException | IllegalArgumentException |
                        SecurityException e) {
                    // No options left. Just return null.
                }
            }
            return result;
        }


        private static class PrivilegedLoadDefault implements PrivilegedAction<Configurator> {

            @Override
            public Configurator run() {
                return Configurator.loadDefault();
            }
        }


        public String getNegotiatedSubprotocol(List<String> supported,
                List<String> requested) {
            return fetchContainerDefaultConfigurator().getNegotiatedSubprotocol(supported, requested);
        }

        public List<Extension> getNegotiatedExtensions(List<Extension> installed,
                List<Extension> requested) {
            return fetchContainerDefaultConfigurator().getNegotiatedExtensions(installed, requested);
        }

        /**
         * websocket建立前确认http升级请求中的origin请求头值
         */
        public boolean checkOrigin(String originHeaderValue) {
            return fetchContainerDefaultConfigurator().checkOrigin(originHeaderValue);
        }

        /**
         * websocket握手处理...
         */
        public void modifyHandshake(ServerEndpointConfig sec,
                HandshakeRequest request, HandshakeResponse response) {
            fetchContainerDefaultConfigurator().modifyHandshake(sec, request, response);
        }

        /**
         * 根据websocket服务端类创建实例
         */
        public <T extends Object> T getEndpointInstance(Class<T> clazz)
                throws InstantiationException {
            return fetchContainerDefaultConfigurator().getEndpointInstance(
                    clazz);
        }
    }
}
