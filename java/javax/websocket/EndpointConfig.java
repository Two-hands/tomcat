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

import java.util.List;
import java.util.Map;


/**
 *  websocket端点（服务端、客户端）的配置
 */
public interface EndpointConfig {

    /**
     * 获取websocket端点通讯时的编码器
     * @return 编码器集合
     */
    List<Class<? extends Encoder>> getEncoders();

    /**
     * 获取websocket端点通讯时的解码器
     * @return 解码器集合
     */
    List<Class<? extends Decoder>> getDecoders();

    /**
     * 获取websocket需要的额外配置信息
     * @return 配置信息
     */
    Map<String,Object> getUserProperties();
}
