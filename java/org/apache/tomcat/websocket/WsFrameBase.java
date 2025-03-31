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
package org.apache.tomcat.websocket;

import org.apache.juli.logging.Log;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.buf.Utf8Decoder;
import org.apache.tomcat.util.res.StringManager;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Extension;
import javax.websocket.MessageHandler;
import javax.websocket.PongMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Takes the ServletInputStream, processes the WebSocket frames it contains and
 * extracts the messages. WebSocket Pings received will be responded to
 * automatically without any action required by the application.
 */
public abstract class WsFrameBase {

    private static final StringManager sm = StringManager.getManager(WsFrameBase.class);

    // 当前socket关联的session
    protected final WsSession wsSession;

    //输入缓冲区（默认8kB）...
    protected final ByteBuffer inputBuffer;
    private final Transformation transformation;

    // Attributes for control messages
    // Control messages can appear in the middle of other messages so need
    // separate attributes
    private final ByteBuffer controlBufferBinary = ByteBuffer.allocate(125);
    private final CharBuffer controlBufferText = CharBuffer.allocate(125);

    // Attributes of the current message
    private final CharsetDecoder utf8DecoderControl = new Utf8Decoder().
            onMalformedInput(CodingErrorAction.REPORT).
            onUnmappableCharacter(CodingErrorAction.REPORT);
    private final CharsetDecoder utf8DecoderMessage = new Utf8Decoder().
            onMalformedInput(CodingErrorAction.REPORT).
            onUnmappableCharacter(CodingErrorAction.REPORT);
    private boolean continuationExpected = false;
    private boolean textMessage = false;

    //消息二进制缓冲区，存放二进制消息数据（由WsWebSocketContainer#getDefaultMaxBinaryMessageBufferSize决定）
    private ByteBuffer messageBufferBinary;

    //消息文本缓冲区，存放文本消息数据（由WsWebSocketContainer#getDefaultMaxTextMessageBufferSize决定）
    private CharBuffer messageBufferText;
    // Cache the message handler in force when the message starts so it is used
    // consistently for the entire message
    private MessageHandler binaryMsgHandler = null;
    private MessageHandler textMsgHandler = null;

    // 帧的属性：标志完整消息的最后一帧（占一个位：1当前帧为结束，0后续还有帧）
    //帧的第一个字节的位置：1000 0000
    private boolean fin = false;

    //帧的属性：保留字段（占3个位）
    //帧的第一个字节的位置：0111 0000
    private int rsv = 0;

    //帧的属性：控制码（占4个位）
    //帧的第一个字节的位置：0000 1111
    //延续(0)，文本(1)，二进制(2)，连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)
    // [3 - 7] 与 [11 - 15] 为保留值....
    private byte opCode = 0;

    //帧的属性：掩码（占1位）用于客户端发送给服务端的数据的解密（掩码有4个字节）
    // 帧的第二个字节的位置：1000 0000
    private final byte[] mask = new byte[4];

    //当
    private int maskIndex = 0;

    //当前帧中消息数据的字节长度
    private long payloadLength = 0;

    //已经读取当前帧中的消息数据字节数
    private volatile long payloadWritten = 0;

    // Attributes tracking state
    //记录帧的解析状态：
    // State.NEW_FRAME - 开始解析新的帧：获取帧的控制信息
    // State.PARTIAL_HEADER - 解析帧的头：获取帧的掩码和含数据大小
    // State.DATA - 解析帧的消息：开始读取帧的具体数据
    private volatile State state = State.NEW_FRAME;
    private volatile boolean open = true;

    private static final AtomicReferenceFieldUpdater<WsFrameBase, ReadState> READ_STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(WsFrameBase.class, ReadState.class, "readState");
    private volatile ReadState readState = ReadState.WAITING;

    public WsFrameBase(WsSession wsSession, Transformation transformation) {
        inputBuffer = ByteBuffer.allocate(Constants.DEFAULT_BUFFER_SIZE);
        inputBuffer.position(0).limit(0);
        messageBufferBinary = ByteBuffer.allocate(wsSession.getMaxBinaryMessageBufferSize());
        messageBufferText = CharBuffer.allocate(wsSession.getMaxTextMessageBufferSize());
        wsSession.setWsFrame(this);
        this.wsSession = wsSession;
        Transformation finalTransformation;
        if (isMasked()) {
            finalTransformation = new UnmaskTransformation();
        } else {
            finalTransformation = new NoopTransformation();
        }
        if (transformation == null) {
            this.transformation = finalTransformation;
        } else {
            transformation.setNext(finalTransformation);
            this.transformation = transformation;
        }
    }


    /**
     * 处理inputBuffer缓冲区的数据
     */

    protected void processInputBuffer() throws IOException {
        while (!isSuspended()) {
            //更新时间[更新session的最近修改时间为当前时间]
            wsSession.updateLastActiveRead();

            //处理新帧
            if (state == State.NEW_FRAME) {
                //1、解析帧的头（前2个字节）的fin、rsv、opCode、mask、payload
                if (!processInitialHeader()) {
                    //缓冲区不够2个字节，终止解析
                    break;
                }

                //已经关闭，不再处理
                if (!open) {
                    throw new IOException(sm.getString("wsFrame.closed"));
                }
            }

            //2、获取帧头中的4个字节的掩码秘钥（有的话）、数据载荷值
            if (state == State.PARTIAL_HEADER) {
                if (!processRemainingHeader()) {
                    //缓冲区不够n个字节，终止解析
                    //n = 4byte（服务端解析时需要帧含掩码） + payload（0byte，2byte,8byte）
                    break;
                }
            }

            //3、获取帧的真正的数据部分
            if (state == State.DATA) {
                // processData() - 处理解析帧的数据，返回true - 帧解析完毕；false - 帧数据还未解析完，需要从socket缓冲区获取剩余数据
                if (!processData()) {
                    //inputBuffer缓冲区数据已空，终止当前处理，再次从socket缓冲区接收数据到inputBuffer
                    break;
                }
            }
        }
    }


    /**
     * 读取帧的前2个字节，获取：结束标志（fin），保留字段（srv），控制码（opCode），消息数据长度（payload length）
     * @return 解析后是否需要进行后续的帧头解析？true - 是，false - 终止解析
     */
    private boolean processInitialHeader() throws IOException {
        //处理帧的头部至少需要2个字节
        if (inputBuffer.remaining() < 2) {
            return false;
        }

        //解析第一个字节：
        int b = inputBuffer.get();
        // fin - 结束标志，占1位 [b & 1000 0000]
        //    - 0 当前帧只有部分数据
        //    - 1 当前帧包含完整数据
        fin = (b & 0x80) != 0;
        // rsv（rsv1、rsv2、rsv3） - 保留位，占3位 [b & 0111 0000]
        rsv = (b & 0x70) >>> 4;
        // opCode - 帧的类型，占4位 [b & 0000 1111]
        //        - 类型枚举：延续(0)，文本(1)，二进制(2)，连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)
        opCode = (byte) (b & 0x0F);

        //rsv的值必须为0
        if (!transformation.validateRsv(rsv, opCode)) {
            throw new WsIOException(new CloseReason(
                    CloseCodes.PROTOCOL_ERROR,
                    sm.getString("wsFrame.wrongRsv", Integer.valueOf(rsv), Integer.valueOf(opCode))));
        }

        if (Util.isControl(opCode)) {
            // 控制帧，此时：opCode >= 8 [连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)]
            if (!fin) {
                //控制帧必须是一个完整的帧，不能分割
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlFragmented")));
            }

            //当前控制帧当前只支持：连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)
            if (opCode != Constants.OPCODE_PING &&
                    opCode != Constants.OPCODE_PONG &&
                    opCode != Constants.OPCODE_CLOSE) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.invalidOpCode", Integer.valueOf(opCode))));
            }
        } else {
            //数据帧，此时：opCode < 8 [延续(0)，文本(1)，二进制(2)]
            if (continuationExpected) {
                //当前帧为连续帧[延续(0)]，帧数据的数据类型（二进制/文本）由第一个帧指定
                if (!Util.isContinuation(opCode)) {
                    throw new WsIOException(new CloseReason(
                            CloseCodes.PROTOCOL_ERROR,
                            sm.getString("wsFrame.noContinuation")));
                }
            } else {
                try {
                    //当前帧不是连续帧，或者当前帧是连续帧的首帧
                    // 数据类型：文本(1)，二进制(2)
                    if (opCode == Constants.OPCODE_BINARY) {
                        //帧携带的数据是二进制帧
                        textMessage = false;

                        //开辟缓冲区，获取二进制消息处理器
                        int size = wsSession.getMaxBinaryMessageBufferSize();
                        if (size != messageBufferBinary.capacity()) {
                            messageBufferBinary = ByteBuffer.allocate(size);
                        }
                        binaryMsgHandler = wsSession.getBinaryMessageHandler();
                        textMsgHandler = null;
                    } else if (opCode == Constants.OPCODE_TEXT) {
                        //帧携带的数据是文本帧
                        textMessage = true;

                        //开辟缓冲区，获取文本消息处理器
                        int size = wsSession.getMaxTextMessageBufferSize();
                        if (size != messageBufferText.capacity()) {
                            messageBufferText = CharBuffer.allocate(size);
                        }
                        binaryMsgHandler = null;
                        textMsgHandler = wsSession.getTextMessageHandler();
                    } else {
                        // 无效opCode值
                        throw new WsIOException(new CloseReason(
                                CloseCodes.PROTOCOL_ERROR,
                                sm.getString("wsFrame.invalidOpCode", Integer.valueOf(opCode))));
                    }
                } catch (IllegalStateException ise) {
                    throw new WsIOException(new CloseReason(
                            CloseCodes.PROTOCOL_ERROR,
                            sm.getString("wsFrame.sessionClosed")));
                }
            }

            //标记是否为连续帧？fin=false时表示为帧数据未结束
            // continuationExpected=true 数据被分散在多个帧中
            continuationExpected = !fin;
        }

        //解析第二个字节：
        b = inputBuffer.get();
        // mask - 是否含计算掩码，占1位[b & 1000 0000]值为0-127
        //      0 - 不含
        //      1 - 帧含有4个字节的计算掩码
        //isMasked - client端解析时为false，server端解析时为true，即客户端发送的帧计算掩码必须是1
        if ((b & 0x80) == 0 && isMasked()) {
            //客户端发送的帧，其mask位必须是1
            throw new WsIOException(new CloseReason(
                    CloseCodes.PROTOCOL_ERROR,
                    sm.getString("wsFrame.notMasked")));
        }

        // payload - 帧数据的有效载荷（真正的数据部分的长度），帧7位[b & 0111 1111]
        payloadLength = b & 0x7F;

        //标记帧头的2个字节信息已经解析完成
        state = State.PARTIAL_HEADER;
        if (getLog().isDebugEnabled()) {
            getLog().debug(sm.getString("wsFrame.partialHeaderComplete", Boolean.toString(fin),
                    Integer.toString(rsv), Integer.toString(opCode), Long.toString(payloadLength)));
        }
        return true;
    }


    protected abstract boolean isMasked();
    protected abstract Log getLog();


    /**
     * 解析帧头扩展部分：掩码秘钥、消息数据长度
     * @return 解析是否完成，可以进行后续的消息数据？ true - 完成，进行后续消息解析；false - 数据不完整，终止
     */
    private boolean processRemainingHeader() throws IOException {
        //需要忽略帧的前2个字节（已经读取完） + 4个字节（客户端发送的帧需要携带掩码秘钥）
        int headerLength;

        //marking-key  - 掩码秘钥，占4个字节（客户端发送给服务端的帧需要携带掩码。对帧携带的真正数据进行XOR掩码操作）
        if (isMasked()) {
            //客户端端发送的帧，需要掩码秘钥
            headerLength = 4;
        } else {
            //服务端发送的帧，不需要掩码秘钥
            headerLength = 0;
        }

        //payload - 帧数据的有效载荷（真正的数据部分的长度）
        //  1、 payload length < 126时，payloadLength值为当前的报文数据长度
        //  2、 payload length = 126时，payloadLength紧接着的后2个字节的值为报文数据长度
        //  3、 payload length = 127时，payloadLength紧接着的后8个字节的值为报文数据长度
        if (payloadLength == 126) {
            headerLength += 2;
        } else if (payloadLength == 127) {
            headerLength += 8;
        }

        //缓冲区字节数不够
        if (inputBuffer.remaining() < headerLength) {
            return false;
        }

        //获取有效数据载荷的具体数值
        if (payloadLength == 126) {
            //读取帧的头2个字节后紧跟着的2个字节
            payloadLength = byteArrayToLong(inputBuffer.array(),
                    inputBuffer.arrayOffset() + inputBuffer.position(), 2);
            inputBuffer.position(inputBuffer.position() + 2);
        } else if (payloadLength == 127) {
            //读取帧的头2个字节后紧跟着的4个字节
            payloadLength = byteArrayToLong(inputBuffer.array(),
                    inputBuffer.arrayOffset() + inputBuffer.position(), 8);

            //值必须是非负数
            if (payloadLength < 0) {
                throw new WsIOException(
                        new CloseReason(CloseCodes.PROTOCOL_ERROR, sm.getString("wsFrame.payloadMsbInvalid")));
            }
            inputBuffer.position(inputBuffer.position() + 8);
        }


        if (Util.isControl(opCode)) {
            //控制帧，不能携带过多数据
            if (payloadLength > 125) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlPayloadTooBig", Long.valueOf(payloadLength))));
            }

            if (!fin) {
                //控制帧只能是单个帧
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlNoFin")));
            }
        }

        //获4个字节的取掩码秘钥（在帧的头2个字节 + 后payload所占的字节后面）
        if (isMasked()) {
            inputBuffer.get(mask, 0, 4);
        }

        //帧头已经解析完，准备解析帧的数据部分
        state = State.DATA;
        return true;
    }


    private boolean processData() throws IOException {
        boolean result;
        if (Util.isControl(opCode)) {
            //处理控制帧，如：关闭、Ping、Pong
            result = processDataControl();
        } else if (textMessage) {
            //帧的数据类型为文本
            if (textMsgHandler == null) {
                result = swallowInput();
            } else {
                //处理文本帧数据
                result = processDataText();
            }
        } else {
            //帧的数据类型为二进制
            if (binaryMsgHandler == null) {
                result = swallowInput();
            } else {
                //处理二进制帧
                result = processDataBinary();
            }
        }
        if (result) {
            updateStats(payloadLength);
        }
        checkRoomPayload();
        return result;
    }


    /**
     * Hook for updating server side statistics. Called on every frame received.
     *
     * @param payloadLength Size of message payload
     */
    protected void updateStats(long payloadLength) {
        // NO-OP by default
    }


    private boolean processDataControl() throws IOException {
        TransformationResult tr = transformation.getMoreData(opCode, fin, rsv, controlBufferBinary);
        if (TransformationResult.UNDERFLOW.equals(tr)) {
            return false;
        }
        // Control messages have fixed message size so
        // TransformationResult.OVERFLOW is not possible here

        controlBufferBinary.flip();
        if (opCode == Constants.OPCODE_CLOSE) {
            open = false;
            String reason = null;
            int code = CloseCodes.NORMAL_CLOSURE.getCode();
            if (controlBufferBinary.remaining() == 1) {
                controlBufferBinary.clear();
                // Payload must be zero or 2+ bytes long
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.oneByteCloseCode")));
            }
            if (controlBufferBinary.remaining() > 1) {
                //获取控制帧的数据部分，解码
                code = controlBufferBinary.getShort();
                if (controlBufferBinary.remaining() > 0) {
                    CoderResult cr = utf8DecoderControl.decode(controlBufferBinary,
                            controlBufferText, true);
                    if (cr.isError()) {
                        controlBufferBinary.clear();
                        controlBufferText.clear();
                        throw new WsIOException(new CloseReason(
                                CloseCodes.PROTOCOL_ERROR,
                                sm.getString("wsFrame.invalidUtf8Close")));
                    }
                    controlBufferText.flip();
                    //解码结果，关闭原因
                    reason = controlBufferText.toString();
                }
            }
            //关闭当前websocket连接，调用wsServer.onClose，传递关闭原因
            wsSession.onClose(new CloseReason(Util.getCloseCode(code), reason));
        } else if (opCode == Constants.OPCODE_PING) {
            if (wsSession.isOpen()) {
                wsSession.getBasicRemote().sendPong(controlBufferBinary);
            }
        } else if (opCode == Constants.OPCODE_PONG) {
            MessageHandler.Whole<PongMessage> mhPong = wsSession.getPongMessageHandler();
            if (mhPong != null) {
                try {
                    mhPong.onMessage(new WsPongMessage(controlBufferBinary));
                } catch (Throwable t) {
                    handleThrowableOnSend(t);
                } finally {
                    controlBufferBinary.clear();
                }
            }
        } else {
            // Should have caught this earlier but just in case...
            controlBufferBinary.clear();
            throw new WsIOException(new CloseReason(
                    CloseCodes.PROTOCOL_ERROR,
                    sm.getString("wsFrame.invalidOpCode", Integer.valueOf(opCode))));
        }
        controlBufferBinary.clear();
        newFrame();
        return true;
    }


    @SuppressWarnings("unchecked")
    protected void sendMessageText(boolean last) throws WsIOException {
        if (textMsgHandler instanceof WrappedMessageHandler) {
            long maxMessageSize = ((WrappedMessageHandler) textMsgHandler).getMaxMessageSize();
            if (maxMessageSize > -1 && messageBufferText.remaining() > maxMessageSize) {
                throw new WsIOException(new CloseReason(CloseCodes.TOO_BIG,
                        sm.getString("wsFrame.messageTooBig",
                                Long.valueOf(messageBufferText.remaining()),
                                Long.valueOf(maxMessageSize))));
            }
        }

        try {
            if (textMsgHandler instanceof MessageHandler.Partial<?>) {
                ((MessageHandler.Partial<String>) textMsgHandler)
                        .onMessage(messageBufferText.toString(), last);
            } else {
                // Caller ensures last == true if this branch is used
                ((MessageHandler.Whole<String>) textMsgHandler)
                        .onMessage(messageBufferText.toString());
            }
        } catch (Throwable t) {
            handleThrowableOnSend(t);
        } finally {
            messageBufferText.clear();
        }
    }


    /**
     * 解析当前帧的数据，将其转为文本
     * @return 当前帧是否解析完毕？ true - 解析完毕， false - 解析未完成，需要继续从socket缓冲区拿数据
     */
    private boolean processDataText() throws IOException {
        // 将inputBuffer缓冲区数据通过mask解码后放入messageBufferBinary缓冲区，会出现3中情况：
        //    1、TransformationResult.END_OF_FRAME  —— 当前帧数据读取完成
        //    2、TransformationResult.UNDERFLOW     —— 源inputBuffer缓冲区没有可读数据[帧数据还未读完，需要等待更多的数据]
        //    3、TransformationResult.OVERFLOW      —— 目标messageBufferBinary缓冲区空间已满[需要将数据编码消耗后再获取数据]
        TransformationResult tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);


        //当前帧数据未读完，会进入当前循环
        while (!TransformationResult.END_OF_FRAME.equals(tr)) {
            //切换为写模式
            messageBufferBinary.flip();
            while (true) {
                //数据解码[从messageBufferBinary拿出二进制数据解码成文本后放入messageBufferText]
                CoderResult cr = utf8DecoderMessage.decode(messageBufferBinary, messageBufferText,
                        false);

                if (cr.isError()) {
                    //解码错误
                    throw new WsIOException(new CloseReason(
                            CloseCodes.NOT_CONSISTENT,
                            sm.getString("wsFrame.invalidUtf8")));
                } else if (cr.isOverflow()) {
                    // 文本缓冲区messageBufferText已满，发送[清空]后再循环解码
                    // usePartial - 是否可以发送部分数据？默认不允许
                    if (usePartial()) {
                        messageBufferText.flip();
                        sendMessageText(false);
                        messageBufferText.clear();
                    } else {
                        throw new WsIOException(new CloseReason(
                                CloseCodes.TOO_BIG,
                                sm.getString("wsFrame.textMessageTooBig")));
                    }
                } else if (cr.isUnderflow()) {
                    //缓冲区messageBufferText未满，但缓冲区messageBufferBinary已空[需要再次从inputBuffer中获取数据]
                    messageBufferBinary.compact();

                    if (TransformationResult.OVERFLOW.equals(tr)) {
                        //上次读取时缓冲区messageBufferBinary已满，但inputBuffer缓冲区可能还有数据
                        //终止循环解码，再次从inputBuffer中拿数据
                        break;
                    } else {
                        // inputBuffer、messageBufferBinary缓冲区都已空
                        // 可能socket接收缓冲区还有数据，终止文本数据处理，再次尝试从socket接收缓冲区读取数据到inputBuffer缓冲区
                        return false;
                    }
                }
            }
            // 继续从inputBuffer缓冲区拿数据，放入messageBufferBinary缓冲区
            tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);
        }


        //到这里，说明已经读到最后的完整数据
        messageBufferBinary.flip();
        boolean last = false;

        //此时的最后部分数据已经全部达到，使用utf-8字符集进行编码
        while (true) {
            CoderResult cr = utf8DecoderMessage.decode(messageBufferBinary, messageBufferText,
                    last);
            if (cr.isError()) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.NOT_CONSISTENT,
                        sm.getString("wsFrame.invalidUtf8")));
            } else if (cr.isOverflow()) {
                // Ran out of space in text buffer - flush it
                if (usePartial()) {
                    messageBufferText.flip();
                    sendMessageText(false);
                    messageBufferText.clear();
                } else {
                    throw new WsIOException(new CloseReason(
                            CloseCodes.TOO_BIG,
                            sm.getString("wsFrame.textMessageTooBig")));
                }
            } else if (cr.isUnderflow() && !last) {
                //当前帧的数据已经解析完毕
                if (continuationExpected) {
                    //但当前帧的数据不完整[数据在多个帧上]
                    if (usePartial()) {
                        messageBufferText.flip();
                        sendMessageText(false);
                        messageBufferText.clear();
                    }
                    messageBufferBinary.compact();
                    newFrame();
                    //当前帧处理完成，等待下一个帧的数据
                    return true;
                } else {
                    //当前帧是完整的数据
                    last = true;
                }
            } else {
                //当前帧读取解码完毕，发送消息给wsServer
                messageBufferText.flip();

                //** 发送帧的完整数据到wsServer.onMessage
                sendMessageText(true);
                //初始消息格式，等待下一个帧数据到来
                newMessage();
                //当前帧数据完整到达，解析完毕
                return true;
            }
        }
    }


    private boolean processDataBinary() throws IOException {
        // Copy the available data to the buffer
        TransformationResult tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);
        while (!TransformationResult.END_OF_FRAME.equals(tr)) {
            // Frame not complete - what did we run out of?
            if (TransformationResult.UNDERFLOW.equals(tr)) {
                // Ran out of input data - get some more
                return false;
            }

            // Ran out of message buffer - flush it
            if (!usePartial()) {
                CloseReason cr = new CloseReason(CloseCodes.TOO_BIG,
                        sm.getString("wsFrame.bufferTooSmall",
                                Integer.valueOf(messageBufferBinary.capacity()),
                                Long.valueOf(payloadLength)));
                throw new WsIOException(cr);
            }
            messageBufferBinary.flip();
            ByteBuffer copy = ByteBuffer.allocate(messageBufferBinary.limit());
            copy.put(messageBufferBinary);
            copy.flip();
            sendMessageBinary(copy, false);
            messageBufferBinary.clear();
            // Read more data
            tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);
        }

        // Frame is fully received
        // Send the message if either:
        // - partial messages are supported
        // - the message is complete
        if (usePartial() || !continuationExpected) {
            messageBufferBinary.flip();
            ByteBuffer copy = ByteBuffer.allocate(messageBufferBinary.limit());
            copy.put(messageBufferBinary);
            copy.flip();
            sendMessageBinary(copy, !continuationExpected);
            messageBufferBinary.clear();
        }

        if (continuationExpected) {
            // More data for this message expected, start a new frame
            newFrame();
        } else {
            // Message is complete, start a new message
            newMessage();
        }

        return true;
    }


    private void handleThrowableOnSend(Throwable t) throws WsIOException {
        ExceptionUtils.handleThrowable(t);
        wsSession.getLocal().onError(wsSession, t);
        CloseReason cr = new CloseReason(CloseCodes.CLOSED_ABNORMALLY,
                sm.getString("wsFrame.ioeTriggeredClose"));
        throw new WsIOException(cr);
    }


    @SuppressWarnings("unchecked")
    protected void sendMessageBinary(ByteBuffer msg, boolean last) throws WsIOException {
        if (binaryMsgHandler instanceof WrappedMessageHandler) {
            long maxMessageSize = ((WrappedMessageHandler) binaryMsgHandler).getMaxMessageSize();
            if (maxMessageSize > -1 && msg.remaining() > maxMessageSize) {
                throw new WsIOException(new CloseReason(CloseCodes.TOO_BIG,
                        sm.getString("wsFrame.messageTooBig",
                                Long.valueOf(msg.remaining()),
                                Long.valueOf(maxMessageSize))));
            }
        }
        try {
            if (binaryMsgHandler instanceof MessageHandler.Partial<?>) {
                ((MessageHandler.Partial<ByteBuffer>) binaryMsgHandler).onMessage(msg, last);
            } else {
                // Caller ensures last == true if this branch is used
                ((MessageHandler.Whole<ByteBuffer>) binaryMsgHandler).onMessage(msg);
            }
        } catch (Throwable t) {
            handleThrowableOnSend(t);
        }
    }


    private void newMessage() {
        messageBufferBinary.clear();
        messageBufferText.clear();
        utf8DecoderMessage.reset();
        continuationExpected = false;
        newFrame();
    }


    private void newFrame() {
        //没有可读数据，设置position=0,limit=0（无法读）
        if (inputBuffer.remaining() == 0) {
            inputBuffer.position(0).limit(0);
        }

        //恢复帧的各个标志状态....
        maskIndex = 0;
        payloadWritten = 0;
        state = State.NEW_FRAME;

        //若缓冲区剩余空间太少，进行数据压缩，腾出多余的空间供后续的读取：
        // 实现功能如：  x - 已读取数据，+ - 未读取数据  ? - 空位置
        //              checkRoomHeaders
        //   xxx++++???  ———————————————> ++++??????
        // p=4,limit=7,p=10              p=0,limit=4,p=10
        checkRoomHeaders();
    }


    private void checkRoomHeaders() {
        // Is the start of the current frame too near the end of the input
        // buffer?
        //当前输入缓冲区剩余可写的空间太小时，对缓冲区进行压缩：
        //  将当前未读的数据依次向前移动到开头：
        // 如：       position = 3  limit = 8   capacity = 10
        // compact:  position = 5  limit = 10  capacity = 10
        // flip：    position = 0  limit 5  capacity = 10
        // 此时缓冲区可读数据已经移到最开头位置，可以开始从缓冲区中读取数据....
        if (inputBuffer.capacity() - inputBuffer.position() < 131) {
            // Limit based on a control frame with a full payload
            makeRoom();
        }
    }


    private void checkRoomPayload() {
        if (inputBuffer.capacity() - inputBuffer.position() - payloadLength + payloadWritten < 0) {
            makeRoom();
        }
    }


    private void makeRoom() {
        inputBuffer.compact();
        inputBuffer.flip();
    }


    private boolean usePartial() {
        if (Util.isControl(opCode)) {
            return false;
        } else if (textMessage) {
            return textMsgHandler instanceof MessageHandler.Partial;
        } else {
            // Must be binary
            return binaryMsgHandler instanceof MessageHandler.Partial;
        }
    }


    /**
     * 默认处理接收到的websocket帧中的消息体部分
     * @return 是否有数据被解析到？ true - 解析到数据，false - 未解析到数据
     */
    private boolean swallowInput() {
        // payloadLength - payloadWritten：消息还剩多少个字节未读？
        //inputBuffer - 缓存中还剩多少可读的字节数？
        long toSkip = Math.min(payloadLength - payloadWritten, inputBuffer.remaining());
        inputBuffer.position(inputBuffer.position() + (int) toSkip);
        payloadWritten += toSkip;

        //当前帧本数据已读取完毕
        if (payloadWritten == payloadLength) {
            if (continuationExpected) {
                //初始化记录点（初始化当前帧为新帧状态），以继续读取还未结束的下一个帧
                newFrame();
            } else {
                //当前帧已处理完，开始读取新的帧
                newMessage();
            }
            return true;
        } else {
            return false;
        }
    }


    protected static long byteArrayToLong(byte[] b, int start, int len) throws IOException {
        if (len > 8) {
            throw new IOException(sm.getString("wsFrame.byteToLongFail", Long.valueOf(len)));
        }
        int shift = 0;
        long result = 0;
        for (int i = start + len - 1; i >= start; i--) {
            result = result + ((b[i] & 0xFFL) << shift);
            shift += 8;
        }
        return result;
    }


    protected boolean isOpen() {
        return open;
    }


    protected Transformation getTransformation() {
        return transformation;
    }


    private enum State {
        NEW_FRAME, PARTIAL_HEADER, DATA
    }


    /**
     * WAITING            - not suspended
     *                      Server case: waiting for a notification that data
     *                      is ready to be read from the socket, the socket is
     *                      registered to the poller
     *                      Client case: data has been read from the socket and
     *                      is waiting for data to be processed
     * PROCESSING         - not suspended
     *                      Server case: reading from the socket and processing
     *                      the data
     *                      Client case: processing the data if such has
     *                      already been read and more data will be read from
     *                      the socket
     * SUSPENDING_WAIT    - suspended, a call to suspend() was made while in
     *                      WAITING state. A call to resume() will do nothing
     *                      and will transition to WAITING state
     * SUSPENDING_PROCESS - suspended, a call to suspend() was made while in
     *                      PROCESSING state. A call to resume() will do
     *                      nothing and will transition to PROCESSING state
     * SUSPENDED          - suspended
     *                      Server case: processing data finished
     *                      (SUSPENDING_PROCESS) / a notification was received
     *                      that data is ready to be read from the socket
     *                      (SUSPENDING_WAIT), socket is not registered to the
     *                      poller
     *                      Client case: processing data finished
     *                      (SUSPENDING_PROCESS) / data has been read from the
     *                      socket and is available for processing
     *                      (SUSPENDING_WAIT)
     *                      A call to resume() will:
     *                      Server case: register the socket to the poller
     *                      Client case: resume data processing
     * CLOSING            - not suspended, a close will be send
     *
     * <pre>
     *     resume           data to be        resume
     *     no action        processed         no action
     *  |---------------| |---------------| |----------|
     *  |               v |               v v          |
     *  |  |----------WAITING«--------PROCESSING----|  |
     *  |  |             ^   processing             |  |
     *  |  |             |   finished               |  |
     *  |  |             |                          |  |
     *  | suspend        |                     suspend |
     *  |  |             |                          |  |
     *  |  |          resume                        |  |
     *  |  |    register socket to poller (server)  |  |
     *  |  |    resume data processing (client)     |  |
     *  |  |             |                          |  |
     *  |  v             |                          v  |
     * SUSPENDING_WAIT   |                  SUSPENDING_PROCESS
     *  |                |                             |
     *  | data available |        processing finished  |
     *  |-------------»SUSPENDED«----------------------|
     * </pre>
     */
    protected enum ReadState {
        WAITING           (false),
        PROCESSING        (false),
        SUSPENDING_WAIT   (true),
        SUSPENDING_PROCESS(true),
        SUSPENDED         (true),
        CLOSING           (false);

        private final boolean isSuspended;

        ReadState(boolean isSuspended) {
            this.isSuspended = isSuspended;
        }

        public boolean isSuspended() {
            return isSuspended;
        }
    }

    public void suspend() {
        while (true) {
            switch (readState) {
            case WAITING:
                if (!READ_STATE_UPDATER.compareAndSet(this, ReadState.WAITING,
                        ReadState.SUSPENDING_WAIT)) {
                    continue;
                }
                return;
            case PROCESSING:
                if (!READ_STATE_UPDATER.compareAndSet(this, ReadState.PROCESSING,
                        ReadState.SUSPENDING_PROCESS)) {
                    continue;
                }
                return;
            case SUSPENDING_WAIT:
                if (readState != ReadState.SUSPENDING_WAIT) {
                    continue;
                } else {
                    if (getLog().isWarnEnabled()) {
                        getLog().warn(sm.getString("wsFrame.suspendRequested"));
                    }
                }
                return;
            case SUSPENDING_PROCESS:
                if (readState != ReadState.SUSPENDING_PROCESS) {
                    continue;
                } else {
                    if (getLog().isWarnEnabled()) {
                        getLog().warn(sm.getString("wsFrame.suspendRequested"));
                    }
                }
                return;
            case SUSPENDED:
                if (readState != ReadState.SUSPENDED) {
                    continue;
                } else {
                    if (getLog().isWarnEnabled()) {
                        getLog().warn(sm.getString("wsFrame.alreadySuspended"));
                    }
                }
                return;
            case CLOSING:
                return;
            default:
                throw new IllegalStateException(sm.getString("wsFrame.illegalReadState", state));
            }
        }
    }

    public void resume() {
        while (true) {
            switch (readState) {
            case WAITING:
                if (readState != ReadState.WAITING) {
                    continue;
                } else {
                    if (getLog().isWarnEnabled()) {
                        getLog().warn(sm.getString("wsFrame.alreadyResumed"));
                    }
                }
                return;
            case PROCESSING:
                if (readState != ReadState.PROCESSING) {
                    continue;
                } else {
                    if (getLog().isWarnEnabled()) {
                        getLog().warn(sm.getString("wsFrame.alreadyResumed"));
                    }
                }
                return;
            case SUSPENDING_WAIT:
                if (!READ_STATE_UPDATER.compareAndSet(this, ReadState.SUSPENDING_WAIT,
                        ReadState.WAITING)) {
                    continue;
                }
                return;
            case SUSPENDING_PROCESS:
                if (!READ_STATE_UPDATER.compareAndSet(this, ReadState.SUSPENDING_PROCESS,
                        ReadState.PROCESSING)) {
                    continue;
                }
                return;
            case SUSPENDED:
                if (!READ_STATE_UPDATER.compareAndSet(this, ReadState.SUSPENDED,
                        ReadState.WAITING)) {
                    continue;
                }
                resumeProcessing();
                return;
            case CLOSING:
                return;
            default:
                throw new IllegalStateException(sm.getString("wsFrame.illegalReadState", state));
            }
        }
    }

    protected boolean isSuspended() {
        return readState.isSuspended();
    }

    protected ReadState getReadState() {
        return readState;
    }

    protected void changeReadState(ReadState newState) {
        READ_STATE_UPDATER.set(this, newState);
    }

    protected boolean changeReadState(ReadState oldState, ReadState newState) {
        return READ_STATE_UPDATER.compareAndSet(this, oldState, newState);
    }

    /**
     * This method will be invoked when the read operation is resumed.
     * As the suspend of the read operation can be invoked at any time, when
     * implementing this method one should consider that there might still be
     * data remaining into the internal buffers that needs to be processed
     * before reading again from the socket.
     */
    protected abstract void resumeProcessing();


    private abstract static class TerminalTransformation implements Transformation {

        @Override
        public boolean validateRsvBits(int i) {
            // Terminal transformations don't use RSV bits and there is no next
            // transformation so always return true.
            return true;
        }

        @Override
        public Extension getExtensionResponse() {
            // Return null since terminal transformations are not extensions
            return null;
        }

        @Override
        public void setNext(Transformation t) {
            // NO-OP since this is the terminal transformation
        }

        /**
         * {@inheritDoc}
         * <p>
         * Anything other than a value of zero for rsv is invalid.
         */
        @Override
        public boolean validateRsv(int rsv, byte opCode) {
            return rsv == 0;
        }

        @Override
        public void close() {
            // NO-OP for the terminal transformations
        }
    }


    /**
     * 用于客户端处理服务端发送的消息体数据（无需解密）
     */
    private final class NoopTransformation extends TerminalTransformation {

        @Override
        public TransformationResult getMoreData(byte opCode, boolean fin, int rsv,
                ByteBuffer dest) {
            // opCode is ignored as the transformation is the same for all
            // opCodes
            // rsv is ignored as it known to be zero at this point
            long toWrite = Math.min(payloadLength - payloadWritten, inputBuffer.remaining());
            toWrite = Math.min(toWrite, dest.remaining());

            int orgLimit = inputBuffer.limit();
            inputBuffer.limit(inputBuffer.position() + (int) toWrite);
            dest.put(inputBuffer);
            inputBuffer.limit(orgLimit);
            payloadWritten += toWrite;

            if (payloadWritten == payloadLength) {
                return TransformationResult.END_OF_FRAME;
            } else if (inputBuffer.remaining() == 0) {
                return TransformationResult.UNDERFLOW;
            } else {
                // !dest.hasRemaining()
                return TransformationResult.OVERFLOW;
            }
        }


        @Override
        public List<MessagePart> sendMessagePart(List<MessagePart> messageParts) {
            // TODO Masking should move to this method
            // NO-OP send so simply return the message unchanged.
            return messageParts;
        }
    }



    /**
     * 用于服务端解密客户端发送的消息体数据
     */
    private final class UnmaskTransformation extends TerminalTransformation {

        @Override
        public TransformationResult getMoreData(byte opCode, boolean fin, int rsv,
                ByteBuffer dest) {

            // payloadWritten < payloadLength - 帧中的数据还未读取完
            // inputBuffer.remaining() > 0 - 缓冲区中还有数据可以读取
            // dest.hasRemaining() - 目标缓冲区还可以放数据
            while (payloadWritten < payloadLength && inputBuffer.remaining() > 0 &&
                    dest.hasRemaining()) {
                //按掩码解密，将每个字节数据轮训与4个字节的掩码进行按位异或
                byte b = (byte) ((inputBuffer.get() ^ mask[maskIndex]) & 0xFF);
                maskIndex++;
                if (maskIndex == 4) {
                    maskIndex = 0;
                }
                payloadWritten++;
                dest.put(b);
            }

            //循环结束：
            if (payloadWritten == payloadLength) {
                //1、当前帧的数据读取完毕
                return TransformationResult.END_OF_FRAME;
            } else if (inputBuffer.remaining() == 0) {
                //2、缓冲区没有可读数据（可能还需要从socket接收缓冲区获取更多的数据）
                return TransformationResult.UNDERFLOW;
            } else {
                // 目标缓冲区已满，需要将缓冲区数据使用后再读取
                return TransformationResult.OVERFLOW;
            }
        }

        @Override
        public List<MessagePart> sendMessagePart(List<MessagePart> messageParts) {
            // NO-OP send so simply return the message unchanged.
            return messageParts;
        }
    }
}
