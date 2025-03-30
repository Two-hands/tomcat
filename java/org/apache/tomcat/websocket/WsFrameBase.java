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


    protected void processInputBuffer() throws IOException {
        while (!isSuspended()) {
            //更新session的最近读取时间...
            wsSession.updateLastActiveRead();

            //新的帧数据：
            if (state == State.NEW_FRAME) {
                //1、初始化这帧头（解析帧的前2个字节），校验合法性（含控制信息）
                //这步成功会有：state=State.PARTIAL_HEADER;
                if (!processInitialHeader()) {
                    break;
                }

                //若关闭帧接收到后，后续的数据将不被处理...
                if (!open) {
                    throw new IOException(sm.getString("wsFrame.closed"));
                }
            }

            //2、步骤1成功，开始解析帧头扩展部分，获取掩码和数据长度等信息
            //这步成功会有：state=State.DATA
            if (state == State.PARTIAL_HEADER) {
                if (!processRemainingHeader()) {
                    break;
                }
            }

            //3、步骤2成功，开始解析消息数据（真正的消息）
            if (state == State.DATA) {
                if (!processData()) {
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
        // 至少有2个字节...
        if (inputBuffer.remaining() < 2) {
            return false;
        }
        int b = inputBuffer.get();
        //第一个字节：
        // fin - 结束标志(占1位[x0000000]），1表示当前为消息的最后一帧，0表示消息解析未结束（后面还有数据未到...）
        fin = (b & 0x80) != 0;
        // rsv（rsv1、rsv2、rsv3） - 保留位置作用（占3位[0xxx0000]）
        rsv = (b & 0x70) >>> 4;
        // opCode - 帧类型（占4位[0000xxxx]）：
        //               延续(0)，文本(1)，二进制(2)，连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)
        opCode = (byte) (b & 0x0F);

        if (!transformation.validateRsv(rsv, opCode)) {
            throw new WsIOException(new CloseReason(
                    CloseCodes.PROTOCOL_ERROR,
                    sm.getString("wsFrame.wrongRsv", Integer.valueOf(rsv), Integer.valueOf(opCode))));
        }

        if (Util.isControl(opCode)) {
            // 控制帧，此时：opCode>=8
            if (!fin) {
                //不是最后一帧，而当前帧为：关闭连接、心跳、心跳响应
                // 这种情况不需要分段处理（一个帧即可）
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlFragmented")));
            }
            //opCode>=8时，目前只有3种情况：连接关闭(8)，心跳[ping](9)，心跳响应[pong](10)
            //若不是这三种情况，则opCode非法...
            if (opCode != Constants.OPCODE_PING &&
                    opCode != Constants.OPCODE_PONG &&
                    opCode != Constants.OPCODE_CLOSE) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.invalidOpCode", Integer.valueOf(opCode))));
            }
        } else {
            // 当opCode < 8时，目前只有3种情况：延续(0)，文本(1)，二进制(2)
            if (continuationExpected) {
                //当前帧的数据不完整，还要接收后续的帧（要分段...）
                //若预测到当前帧是分段帧（continuationExpected=true），但opCode!=0,则为异常
                if (!Util.isContinuation(opCode)) {
                    throw new WsIOException(new CloseReason(
                            CloseCodes.PROTOCOL_ERROR,
                            sm.getString("wsFrame.noContinuation")));
                }
            } else {
                try {
                    //到这里只有文本(1)，二进制(2)两种合法操作：
                    if (opCode == Constants.OPCODE_BINARY) {
                        //帧中内容为二进制数据：
                        textMessage = false;
                        int size = wsSession.getMaxBinaryMessageBufferSize();
                        if (size != messageBufferBinary.capacity()) {
                            messageBufferBinary = ByteBuffer.allocate(size);
                        }
                        binaryMsgHandler = wsSession.getBinaryMessageHandler();
                        textMsgHandler = null;
                    } else if (opCode == Constants.OPCODE_TEXT) {
                        //帧中内容为文本数据
                        textMessage = true;
                        int size = wsSession.getMaxTextMessageBufferSize();
                        if (size != messageBufferText.capacity()) {
                            messageBufferText = CharBuffer.allocate(size);
                        }
                        binaryMsgHandler = null;
                        textMsgHandler = wsSession.getTextMessageHandler();
                    } else {
                        //非法opCode
                        throw new WsIOException(new CloseReason(
                                CloseCodes.PROTOCOL_ERROR,
                                sm.getString("wsFrame.invalidOpCode", Integer.valueOf(opCode))));
                    }
                } catch (IllegalStateException ise) {
                    // Thrown if the session is already closed
                    throw new WsIOException(new CloseReason(
                            CloseCodes.PROTOCOL_ERROR,
                            sm.getString("wsFrame.sessionClosed")));
                }
            }

            //如果未发现结束符，则还需要等待剩余的数据到来
            continuationExpected = !fin;
        }


        b = inputBuffer.get();
        //第2个字节：
        //mask - 计算掩码（占1位[x0000000]）值为0-127
        //isMasked - client端解析时为false，server端解析为true
        if ((b & 0x80) == 0 && isMasked()) {
            //当掩码位为0时，server端解析（client端数据必须带计算掩码）报错...
            throw new WsIOException(new CloseReason(
                    CloseCodes.PROTOCOL_ERROR,
                    sm.getString("wsFrame.notMasked")));
        }

        // payload length - 数据长度（占7位[0xxxxxxx]）
        payloadLength = b & 0x7F;

        //标记当前处理状态为"请求头"解析阶段
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
     * @return <code>true</code> if sufficient data was present to complete the
     *         processing of the header
     */

    /**
     * 解析帧头扩展部分：掩码、消息数据长度
     * @return 解析是否完成，可以进行后续的消息数据？ true - 完成，进行后续消息解析；false - 数据不完整，终止
     */
    private boolean processRemainingHeader() throws IOException {
        // Ignore the 2 bytes already read. 4 for the mask
        int headerLength;

        //mark - 掩码（占4个字节），客户端发送给服务端的需要掩码，服务端发生给客户端不需要
        if (isMasked()) {
            headerLength = 4;
        } else {
            headerLength = 0;
        }

        //payload length - 报文消息数据长度
        //  1、 payload length < 126时，值为当前的报文数据长度
        //  2、 payload length = 126时，紧接着的后2个字节的值为报文数据长度
        //  3、 payload length = 127时，紧接着的后8个字节的值为报文数据长度
        if (payloadLength == 126) {
            headerLength += 2;
        } else if (payloadLength == 127) {
            headerLength += 8;
        }

        //当前缓存区字节数不够，返回，不进行解析...
        if (inputBuffer.remaining() < headerLength) {
            return false;
        }

        // Calculate new payload length if necessary
        if (payloadLength == 126) {
            //将payload length的后续2个字节的数据转换为long
            payloadLength = byteArrayToLong(inputBuffer.array(),
                    inputBuffer.arrayOffset() + inputBuffer.position(), 2);
            inputBuffer.position(inputBuffer.position() + 2);
        } else if (payloadLength == 127) {
            //将payload length的后续8个字节的数据转换为long
            payloadLength = byteArrayToLong(inputBuffer.array(),
                    inputBuffer.arrayOffset() + inputBuffer.position(), 8);
            // 这8个字节的最高位必须是0（若是1则为负数，这是不允许的）
            if (payloadLength < 0) {
                throw new WsIOException(
                        new CloseReason(CloseCodes.PROTOCOL_ERROR, sm.getString("wsFrame.payloadMsbInvalid")));
            }
            inputBuffer.position(inputBuffer.position() + 8);
        }


        if (Util.isControl(opCode)) {
            //分段帧校验
            if (payloadLength > 125) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlPayloadTooBig", Long.valueOf(payloadLength))));
            }
            if (!fin) {
                throw new WsIOException(new CloseReason(
                        CloseCodes.PROTOCOL_ERROR,
                        sm.getString("wsFrame.controlNoFin")));
            }
        }

        if (isMasked()) {
            inputBuffer.get(mask, 0, 4);
        }
        state = State.DATA;
        return true;
    }


    private boolean processData() throws IOException {
        boolean result;
        if (Util.isControl(opCode)) {
            result = processDataControl();
        } else if (textMessage) {
            //消息数据为文本类型：
            if (textMsgHandler == null) {
                //textMsgHandler == null 当前帧的文本数据还未读取完
                result = swallowInput();
            } else {
                //处理当前帧的数据，编码为文本
                result = processDataText();
            }
        } else {
            //消息数据为二进制类型：
            if (binaryMsgHandler == null) {
                //binaryMsgHandler == null 当前帧的二进制数据还未读取完
                result = swallowInput();
            } else {
                //处理当前帧的数据，二进制数据（图片？....）
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
                    // There will be no overflow as the output buffer is big
                    // enough. There will be no underflow as all the data is
                    // passed to the decoder in a single call.
                    controlBufferText.flip();
                    reason = controlBufferText.toString();
                }
            }
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


    private boolean processDataText() throws IOException {
        // 从输入缓冲区中获取数据，将其存放到字节缓冲区（messageBufferBinary）中：
        // TransformationResult.UNDERFLOW - 输入缓冲区数据已读取完（帧数据还未读取完，等待）
        // TransformationResult.UNDERFLOW - 字节缓冲区已满，刷新后继续编码
        // TransformationResult.END_OF_FRAME - 帧数据已读取完毕
        TransformationResult tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);

        //还没读取完所有数据，循环....
        while (!TransformationResult.END_OF_FRAME.equals(tr)) {
            //模式切换，开始从此缓冲区中获取已存放的数据
            messageBufferBinary.flip();
            while (true) {
                //使用utf-8字符集进行数据解码，将字节数据（messageBufferBinary）转换为文本数据（messageBufferText）
                CoderResult cr = utf8DecoderMessage.decode(messageBufferBinary, messageBufferText,
                        false);

                if (cr.isError()) {
                    throw new WsIOException(new CloseReason(
                            CloseCodes.NOT_CONSISTENT,
                            sm.getString("wsFrame.invalidUtf8")));
                } else if (cr.isOverflow()) {
                    // 字符缓冲器没有可用空间，刷新其中的数据
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
                    // Compact what we have to create as much space as possible
                    //没有可读数据，压缩以获取更多可用空间
                    messageBufferBinary.compact();

                    if (TransformationResult.OVERFLOW.equals(tr)) {
                        //messageBufferBinary还可以存放数据（从inputBuffer中读取），结束循环，继续填充数据
                        break;
                    } else {
                        // messageBufferBinary有足够空间，但当前inputBuffer中数据还不完整，退出等待数据的到来....
                        return false;
                    }
                }
            }
            // 继续读取数据到messageBufferBinary中
            tr = transformation.getMoreData(opCode, fin, rsv, messageBufferBinary);
        }

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
                // End of frame and possible message as well.

                if (continuationExpected) {
                    // If partial messages are supported, send what we have
                    // managed to decode
                    if (usePartial()) {
                        messageBufferText.flip();
                        sendMessageText(false);
                        messageBufferText.clear();
                    }
                    messageBufferBinary.compact();
                    newFrame();
                    // Process next frame
                    return true;
                } else {
                    // Make sure coder has flushed all output
                    last = true;
                }
            } else {
                // End of message
                messageBufferText.flip();
                sendMessageText(true);

                newMessage();
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
            // inputBuffer.remaining() > 0 - 输入缓冲区中还有消息数据未读
            // dest.hasRemaining() - 还可以存储谁
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
            if (payloadWritten == payloadLength) {
                //当前帧的数据读取完毕
                return TransformationResult.END_OF_FRAME;
            } else if (inputBuffer.remaining() == 0) {
                //输入缓冲区没有可读数据，需要再次从socket中获取数据
                return TransformationResult.UNDERFLOW;
            } else {
                // dest没有空间存放数据，需要使用数据、释放数据后再次存放
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
