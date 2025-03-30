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
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;

/**
 * websocket通讯时的编码器，编码后被输出到对端....
 */
public interface Encoder {


    /**
     * 编码器的初始化...
     * @param endpointConfig  websocket端点配置类
     */
     void init(EndpointConfig endpointConfig);

    /**
     * 编码器的销毁...
     */
    void destroy();

    /**
     * 文本类编码器，将通讯内容转换为字符串
     * @param <T> 被编码的内容类型
     */
    interface Text<T> extends Encoder {

        String encode(T object) throws EncodeException;
    }

    /**
     * 文本类字符流编码器，将通讯内容转换为字符流并写入Writer中
     * @param <T> 被编码的内容类型
     */
    interface TextStream<T> extends Encoder {

        void encode(T object, Writer writer)
                throws EncodeException, IOException;
    }

    /**
     * 字节块缓存编码器，将通讯内容转换为字节数组并包装为ByteBuffer
     * @param <T> 被编码的内容类型
     */
    interface Binary<T> extends Encoder {

        ByteBuffer encode(T object) throws EncodeException;
    }

    /**
     * 字节流编码器，将通讯内容转换为字节数组写入OutputStream
     * @param <T> 被编码的内容类型
     */
    interface BinaryStream<T> extends Encoder {

        void encode(T object, OutputStream os)
                throws EncodeException, IOException;
    }
}
