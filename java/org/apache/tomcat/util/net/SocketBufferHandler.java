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

import org.apache.tomcat.util.buf.ByteBufferUtils;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class SocketBufferHandler {

    static SocketBufferHandler EMPTY = new SocketBufferHandler(0, 0, false) {
        @Override
        public void expand(int newSize) {
        }
        /*
         * Http2AsyncParser$FrameCompletionHandler will return incomplete
         * frame(s) to the buffer. If the previous frame (or concurrent write to
         * a stream) triggered a connection close this call would fail with a
         * BufferOverflowException as data can't be returned to a buffer of zero
         * length. Override the method and make it a NO-OP to avoid triggering
         * the exception.
         */
        @Override
        public void unReadReadBuffer(ByteBuffer returnedData) {
        }
    };

    //记录当前[读缓冲区]是否正处于写入状态
    // true  - 处于写状态，可以向读缓冲区放入数据
    // false - 处于读状态，可以从读缓冲区拿出数据
    private volatile boolean readBufferConfiguredForWrite = true;

    //读缓冲区，由SocketProperties决定，默认8KB
    private volatile ByteBuffer readBuffer;

    private volatile boolean writeBufferConfiguredForWrite = true;

    //写缓冲区，由SocketProperties决定，默认8KB
    private volatile ByteBuffer writeBuffer;

    //缓冲区是否为堆外内存？ true - 堆外内存
    private final boolean direct;

    public SocketBufferHandler(int readBufferSize, int writeBufferSize,
            boolean direct) {
        this.direct = direct;
        if (direct) {
            //分配直接读、写内存（DirectByteBuffer）
            readBuffer = ByteBuffer.allocateDirect(readBufferSize);
            writeBuffer = ByteBuffer.allocateDirect(writeBufferSize);
        } else {
            //分配堆读、写内存（HeapByteBuffer）
            readBuffer = ByteBuffer.allocate(readBufferSize);
            writeBuffer = ByteBuffer.allocate(writeBufferSize);
        }
    }


    /**
     * 将[读]缓冲区切换为写状态[准备放入数据]
     */
    public void configureReadBufferForWrite() {
        setReadBufferConfiguredForWrite(true);
    }


    /**
     * 将[读]缓冲区切换为读状态[准备拿出数据]
     */
    public void configureReadBufferForRead() {
        setReadBufferConfiguredForWrite(false);
    }


    /**
     * 设置读缓冲区为写入状态
     * @param readBufferConFiguredForWrite 要转换为写入状态？ true - 写
     */
    private void setReadBufferConfiguredForWrite(boolean readBufferConFiguredForWrite) {

        if (this.readBufferConfiguredForWrite != readBufferConFiguredForWrite) {
            if (readBufferConFiguredForWrite) {
                //缓冲区切换为写状态[准备放入数据]
                int remaining = readBuffer.remaining();
                if (remaining == 0) {
                    readBuffer.clear();
                } else {
                    //缓冲区还有数据，先压缩数据，再写入
                    readBuffer.compact();
                }
            } else {
                //缓冲区切换为读状态[准备拿出数据]
                readBuffer.flip();
            }

            this.readBufferConfiguredForWrite = readBufferConFiguredForWrite;
        }
    }


    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }


    public boolean isReadBufferEmpty() {
        if (readBufferConfiguredForWrite) {
            //写状态时，缓冲器没有任何数据时表示缓冲区为空
            return readBuffer.position() == 0;
        } else {
            //读状态时，缓冲区没有数据可读表示缓冲器为空
            return readBuffer.remaining() == 0;
        }
    }


    public void unReadReadBuffer(ByteBuffer returnedData) {
        if (isReadBufferEmpty()) {
            configureReadBufferForWrite();
            readBuffer.put(returnedData);
        } else {
            int bytesReturned = returnedData.remaining();
            if (readBufferConfiguredForWrite) {
                // Writes always start at position zero
                if ((readBuffer.position() + bytesReturned) > readBuffer.capacity()) {
                    throw new BufferOverflowException();
                } else {
                    // Move the bytes up to make space for the returned data
                    for (int i = 0; i < readBuffer.position(); i++) {
                        readBuffer.put(i + bytesReturned, readBuffer.get(i));
                    }
                    // Insert the bytes returned
                    for (int i = 0; i < bytesReturned; i++) {
                        readBuffer.put(i, returnedData.get());
                    }
                    // Update the position
                    readBuffer.position(readBuffer.position() + bytesReturned);
                }
            } else {
                // Reads will start at zero but may have progressed
                int shiftRequired = bytesReturned - readBuffer.position();
                if (shiftRequired > 0) {
                    if ((readBuffer.capacity() - readBuffer.limit()) < shiftRequired) {
                        throw new BufferOverflowException();
                    }
                    // Move the bytes up to make space for the returned data
                    int oldLimit = readBuffer.limit();
                    readBuffer.limit(oldLimit + shiftRequired);
                    for (int i = readBuffer.position(); i < oldLimit; i++) {
                        readBuffer.put(i + shiftRequired, readBuffer.get(i));
                    }
                } else {
                    shiftRequired = 0;
                }
                // Insert the returned bytes
                int insertOffset = readBuffer.position() + shiftRequired - bytesReturned;
                for (int i = insertOffset; i < bytesReturned + insertOffset; i++) {
                    readBuffer.put(i, returnedData.get());
                }
                readBuffer.position(insertOffset);
            }
        }
    }


    /**
     * 将[写]缓冲区设置为写状态[准备放入数据]
     */
    public void configureWriteBufferForWrite() {
        setWriteBufferConfiguredForWrite(true);
    }


    /**
     * 将[写]缓冲区设置为读状态[准备拿出数据]
     */
    public void configureWriteBufferForRead() {
        setWriteBufferConfiguredForWrite(false);
    }


    /**
     * 设置写缓冲区为写入状态？
     * @param writeBufferConfiguredForWrite  要转换为写入状态？ true - 写
     */
    private void setWriteBufferConfiguredForWrite(boolean writeBufferConfiguredForWrite) {

        if (this.writeBufferConfiguredForWrite != writeBufferConfiguredForWrite) {
            if (writeBufferConfiguredForWrite) {
                //缓冲区切换为写状态[准备放入数据]
                int remaining = writeBuffer.remaining();
                if (remaining == 0) {
                    writeBuffer.clear();
                } else {
                    writeBuffer.compact();
                    writeBuffer.position(remaining);
                    writeBuffer.limit(writeBuffer.capacity());
                }
            } else {
                //缓冲区切换为读状态[准备拿出数据]
                writeBuffer.flip();
            }
            this.writeBufferConfiguredForWrite = writeBufferConfiguredForWrite;
        }
    }


    public boolean isWriteBufferWritable() {
        if (writeBufferConfiguredForWrite) {
            return writeBuffer.hasRemaining();
        } else {
            return writeBuffer.remaining() == 0;
        }
    }


    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }


    public boolean isWriteBufferEmpty() {
        if (writeBufferConfiguredForWrite) {
            return writeBuffer.position() == 0;
        } else {
            return writeBuffer.remaining() == 0;
        }
    }


    /**
     * 重置缓冲区
     */
    public void reset() {
        readBuffer.clear();
        readBufferConfiguredForWrite = true;
        writeBuffer.clear();
        writeBufferConfiguredForWrite = true;
    }


    /**
     * 缓冲区扩容
     * @param newSize 扩容大小
     */
    public void expand(int newSize) {
        configureReadBufferForWrite();
        readBuffer = ByteBufferUtils.expand(readBuffer, newSize);
        configureWriteBufferForWrite();
        writeBuffer = ByteBufferUtils.expand(writeBuffer, newSize);
    }

    public void free() {
        if (direct) {
            ByteBufferUtils.cleanDirectBuffer(readBuffer);
            ByteBufferUtils.cleanDirectBuffer(writeBuffer);
        }
    }

}
