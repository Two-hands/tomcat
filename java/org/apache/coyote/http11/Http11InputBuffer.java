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
package org.apache.coyote.http11;

import org.apache.coyote.CloseNowException;
import org.apache.coyote.InputBuffer;
import org.apache.coyote.Request;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.buf.MessageBytes;
import org.apache.tomcat.util.http.HeaderUtil;
import org.apache.tomcat.util.http.MimeHeaders;
import org.apache.tomcat.util.http.parser.HttpParser;
import org.apache.tomcat.util.net.ApplicationBufferHandler;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.apache.tomcat.util.res.StringManager;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * InputBuffer for HTTP that provides request header parsing as well as transfer
 * encoding.
 */
public class Http11InputBuffer implements InputBuffer, ApplicationBufferHandler {

    // -------------------------------------------------------------- Constants

    private static final Log log = LogFactory.getLog(Http11InputBuffer.class);

    /**
     * The string manager for this package.
     */
    private static final StringManager sm = StringManager.getManager(Http11InputBuffer.class);


    private static final byte[] CLIENT_PREFACE_START =
            "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);

    /**
     * Associated Coyote request.
     */
    private final Request request;


    /**
     * Headers of the associated request.
     */
    private final MimeHeaders headers;


    private final boolean rejectIllegalHeader;

    //是否正处于解析请求头？true - 正处于解析请求头
    private volatile boolean parsingHeader;


    /**
     * Swallow input ? (in the case of an expectation)
     */
    private boolean swallowInput;


    /**
     * The read buffer.
     */
    private ByteBuffer byteBuffer;



    //byteBuffer中请求头数据结尾的位置[请求体开始的位置]
    private int end;


    /**
     * Wrapper that provides access to the underlying socket.
     */
    private SocketWrapperBase<?> wrapper;


    /**
     * Underlying input buffer.
     */
    private InputBuffer inputStreamInputBuffer;


    /**
     * Filter library.
     * Note: Filter[Constants.CHUNKED_FILTER] is always the "chunked" filter.
     */
    private InputFilter[] filterLibrary;


    /**
     * Active filters (in order).
     */
    private InputFilter[] activeFilters;


    /**
     * Index of the last active filter.
     */
    private int lastActiveFilter;


    /**
     * Parsing state - used for non blocking parsing so that
     * when more data arrives, we can pick up where we left off.
     */
    private byte prevChr = 0;

    private byte chr = 0;

    //是否要解析请求行？ true - 需要解析请求行
    private volatile boolean parsingRequestLine;

    // -1 - 当前请求为HTTP/2.0，无需按照HTTP/1.1进行解析（无需按2-6步骤）
    //  0 - 解析请求行的初始状态...
    //  1 - 没有数据到达，无法开始解析请求行
    // <2 - 解析请求行的[请求方式]部分前的准备
    //  2 - 正式解析[请求方式]部分
    //  3 - 解析请求行的[URI]部分前的准备
    //  4 - 正式解析[URI]部分
    //  5 - 解析请求行的[协议及版本]部分前的准备
    //  6 - 正式解析[协议及版本]部分
    //  7 - 请求行【解析完成】
    private int parsingRequestLinePhase = 0;

    //请求行是否解析结束 true - 结束
    private boolean parsingRequestLineEol = false;

    private int parsingRequestLineStart = 0;
    private int parsingRequestLineQPos = -1;
    private HeaderParsePosition headerParsePos;
    private final HeaderParseData headerData = new HeaderParseData();
    private final HttpParser httpParser;


    //HTTP请求行 + 请求头 + 以及它们前面的空行 允许使用的最大字节数，默认16KB
    private final int headerBufferSize;

    /**
     * Known size of the NioChannel read buffer.
     */
    private int socketReadBufferSize;


    // ----------------------------------------------------------- Constructors

    public Http11InputBuffer(Request request, int headerBufferSize,
            boolean rejectIllegalHeader, HttpParser httpParser) {

        this.request = request;
        headers = request.getMimeHeaders();

        this.headerBufferSize = headerBufferSize;
        this.rejectIllegalHeader = rejectIllegalHeader;
        this.httpParser = httpParser;

        filterLibrary = new InputFilter[0];
        activeFilters = new InputFilter[0];
        lastActiveFilter = -1;

        parsingHeader = true;
        parsingRequestLine = true;
        parsingRequestLinePhase = 0;
        parsingRequestLineEol = false;
        parsingRequestLineStart = 0;
        parsingRequestLineQPos = -1;
        headerParsePos = HeaderParsePosition.HEADER_START;
        swallowInput = true;

        inputStreamInputBuffer = new SocketInputBuffer();
    }


    // ------------------------------------------------------------- Properties

    /**
     * Add an input filter to the filter library.
     *
     * @throws NullPointerException if the supplied filter is null
     */
    void addFilter(InputFilter filter) {

        if (filter == null) {
            throw new NullPointerException(sm.getString("iib.filter.npe"));
        }

        InputFilter[] newFilterLibrary = Arrays.copyOf(filterLibrary, filterLibrary.length + 1);
        newFilterLibrary[filterLibrary.length] = filter;
        filterLibrary = newFilterLibrary;

        activeFilters = new InputFilter[filterLibrary.length];
    }


    /**
     * Get filters.
     */
    InputFilter[] getFilters() {
        return filterLibrary;
    }


    /**
     * Add an input filter to the filter library.
     */
    void addActiveFilter(InputFilter filter) {

        if (lastActiveFilter == -1) {
            filter.setBuffer(inputStreamInputBuffer);
        } else {
            for (int i = 0; i <= lastActiveFilter; i++) {
                if (activeFilters[i] == filter) {
                    return;
                }
            }
            filter.setBuffer(activeFilters[lastActiveFilter]);
        }

        activeFilters[++lastActiveFilter] = filter;

        filter.setRequest(request);
    }


    /**
     * Set the swallow input flag.
     */
    void setSwallowInput(boolean swallowInput) {
        this.swallowInput = swallowInput;
    }


    // ---------------------------------------------------- InputBuffer Methods

    @Override
    public int doRead(ApplicationBufferHandler handler) throws IOException {
        if (lastActiveFilter == -1) {
            return inputStreamInputBuffer.doRead(handler);
        } else {
            return activeFilters[lastActiveFilter].doRead(handler);
        }
    }


    // ------------------------------------------------------- Protected Methods

    /**
     * Recycle the input buffer. This should be called when closing the
     * connection.
     */
    void recycle() {
        wrapper = null;
        request.recycle();

        for (int i = 0; i <= lastActiveFilter; i++) {
            activeFilters[i].recycle();
        }

        byteBuffer.limit(0).position(0);
        lastActiveFilter = -1;
        swallowInput = true;

        chr = 0;
        prevChr = 0;
        headerParsePos = HeaderParsePosition.HEADER_START;
        parsingRequestLinePhase = 0;
        parsingRequestLineEol = false;
        parsingRequestLineStart = 0;
        parsingRequestLineQPos = -1;
        headerData.recycle();
        // Recycled last because they are volatile
        // All variables visible to this thread are guaranteed to be visible to
        // any other thread once that thread reads the same volatile. The first
        // action when parsing input data is to read one of these volatiles.
        parsingRequestLine = true;
        parsingHeader = true;
    }


    /**
     * End processing of current HTTP request.
     * Note: All bytes of the current request should have been already
     * consumed. This method only resets all the pointers so that we are ready
     * to parse the next HTTP request.
     */
    void nextRequest() {
        request.recycle();

        if (byteBuffer.position() > 0) {
            if (byteBuffer.remaining() > 0) {
                // Copy leftover bytes to the beginning of the buffer
                byteBuffer.compact();
                byteBuffer.flip();
            } else {
                // Reset position and limit to 0
                byteBuffer.position(0).limit(0);
            }
        }

        // Recycle filters
        for (int i = 0; i <= lastActiveFilter; i++) {
            activeFilters[i].recycle();
        }

        // Reset pointers
        lastActiveFilter = -1;
        parsingHeader = true;
        swallowInput = true;

        headerParsePos = HeaderParsePosition.HEADER_START;
        parsingRequestLine = true;
        parsingRequestLinePhase = 0;
        parsingRequestLineEol = false;
        parsingRequestLineStart = 0;
        parsingRequestLineQPos = -1;
        headerData.recycle();
    }



    /**
     * <pre>
     * 解析请求行（包括：请求方式、请求URI[可能含有请求参数]、协议及版本号），请求行只含ascii码
     * <b>注意：请求行内容开头和结尾不能含有空格。开头可以含有换行符，结尾必含换行符</b>
     * @return 请求行是否解析成功？ true - 解析成功： 1、parsingRequestLine=false，2、真正解析完；
     *                          false - 解析失败：1、请求行数据尚未完整到达，2、当前请求是HTTP/2.0
     *</pre>
     */
    boolean parseRequestLine(boolean keptAlive, int connectionTimeout, int keepAliveTimeout)
            throws IOException {

        //无需解析请求行，跳过解析过程
        if (!parsingRequestLine) {
            return true;
        }

        //1、解析请求行的[请求方式]部分前的准备，即：跳过请求行前面的所有换行符
        if (parsingRequestLinePhase < 2) {
            do {

                //必要时，填充数据到byteBuffer
                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (keptAlive) {
                        //避免过长时间没接收到数据或是客户端已关闭
                        wrapper.setReadTimeout(keepAliveTimeout);
                    }
                    //fill : 从socket接收缓冲区中获取数据，放入byteBuffer中
                    //参数false：当前读取操作为非阻塞
                    if (!fill(false)) {
                        parsingRequestLinePhase = 1;
                        return false;
                    }
                    //接收到数据后设置读超时时间
                    wrapper.setReadTimeout(connectionTimeout);
                }

                //每次重新读取数据后都要判断协议是否为HTTP/2.0
                if (!keptAlive && byteBuffer.position() == 0 && byteBuffer.limit() >= CLIENT_PREFACE_START.length - 1) {
                    boolean prefaceMatch = true;
                    //CLIENT_PREFACE_START = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"（属于连接前言内容
                    //主要是用于识别是否是HTTP/2
                    for (int i = 0; i < CLIENT_PREFACE_START.length && prefaceMatch; i++) {
                        if (CLIENT_PREFACE_START[i] != byteBuffer.get(i)) {
                            prefaceMatch = false;
                        }
                    }
                    if (prefaceMatch) {
                        // 请求协议升级为HTTP/2.0
                        parsingRequestLinePhase = -1;
                        return false;
                    }
                }


                //设置当前时间为开始读取报文时间
                if (request.getStartTime() < 0) {
                    request.setStartTime(System.currentTimeMillis());
                }

                //如果读取的内容首字节为换行符（Unix、Linux下是LF，Windows是CR LF），则为空行，再次重Socket中读取后续的数据
                chr = byteBuffer.get();
            } while ((chr == Constants.CR) || (chr == Constants.LF));

            //首个非换行符被拿出来比较过，所以要把缓冲区指针回滚
            byteBuffer.position(byteBuffer.position() - 1);

            //记录当前缓冲区中的请求行的开始位置
            parsingRequestLineStart = byteBuffer.position();
            parsingRequestLinePhase = 2;
        }


        //2、正式解析请求行的[请求方式]部分，如：GET、POST
        //请求方式部分内容只能含：!#$%&'*+-.^_`|~0-9A-Za-z
        //直到读取到空格位置（不算空格）
        if (parsingRequestLinePhase == 2) {
            boolean space = false;

            //读到空隔为止
            while (!space) {

                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (!fill(false)) {
                        return false;
                    }
                }

                int pos = byteBuffer.position();
                chr = byteBuffer.get();
                //SP：空格；HT：制表符
                //读取到空格或制表符就终止（可以是多个空格、制表符），前面部分就是请求方法
                if (chr == Constants.SP || chr == Constants.HT) {
                    //已经读取到了Request的method，终止读取....
                    space = true;
                    //设置Request的method
                    request.method().setBytes(byteBuffer.array(), parsingRequestLineStart,
                            pos - parsingRequestLineStart);
                } else if (!HttpParser.isToken(chr)) {
                    //字符不在 !#$%&'*+-.^_`|~0-9A-Za-z 中
                    request.protocol().setString(Constants.HTTP_11);
                    String invalidMethodValue = parseInvalid(parsingRequestLineStart, byteBuffer);
                    throw new IllegalArgumentException(sm.getString("iib.invalidmethod", invalidMethodValue));
                }
            }
            parsingRequestLinePhase = 3;
        }

        //3、解析请求行的[URI]部分前的准备，即：跳过请求行中[请求方式]后紧接着的所有空格、制表符
        if (parsingRequestLinePhase == 3) {
            boolean space = true;
            while (space) {

                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (!fill(false)) {
                        return false;
                    }
                }


                chr = byteBuffer.get();
                //SP：空格；HT：制表符
                //读取到空格或制表符就终止（可以是多个空格、制表符），前面部分就是请求方法
                if (!(chr == Constants.SP || chr == Constants.HT)) {
                    space = false;
                    byteBuffer.position(byteBuffer.position() - 1);
                }
            }

            // 记录请求行中URI内容的开始位置
            parsingRequestLineStart = byteBuffer.position();
            parsingRequestLinePhase = 4;
        }

        //4、正式解析请求行的[URI]部分，如：/test?key=value
        //URI部分只能含：$%&'()*+,-./0-9:;=?@A-Z[]_a-z~
        //URI部分的请求参数只能含：!$%&'()*+,-./:;=?@~_0-9A-Za-z
        if (parsingRequestLinePhase == 4) {
            int end = 0;

            boolean space = false;
            while (!space) {

                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (!fill(false)) {
                        return false;
                    }
                }


                int pos = byteBuffer.position();
                prevChr = chr;
                chr = byteBuffer.get();
                if (prevChr == Constants.CR && chr != Constants.LF) {
                    // CR后面没有LF，所以不是HTTP/0.9请求
                    request.protocol().setString(Constants.HTTP_11);
                    String invalidRequestTarget = parseInvalid(parsingRequestLineStart, byteBuffer);
                    throw new IllegalArgumentException(sm.getString("iib.invalidRequestTarget", invalidRequestTarget));
                }

                if (chr == Constants.SP || chr == Constants.HT) {
                    //读取的字节为空格、制表符时终止读取....
                    space = true;
                    end = pos;
                } else if (chr == Constants.CR) {
                    // HTTP/0.9：LF是必有，CR是可选的
                } else if (chr == Constants.LF) {
                    space = true;
                    //空格代表 HTTP/0.9
                    request.protocol().setString("");
                    parsingRequestLinePhase = 7;
                    if (prevChr == Constants.CR) {
                        end = pos - 1;
                    } else {
                        end = pos;
                    }
                } else if (chr == Constants.QUESTION && parsingRequestLineQPos == -1) {
                    //QUESTION 就是?
                    //若URI中含'?'，则表明带有参数
                    // 记录请求参数的位置
                    parsingRequestLineQPos = pos;
                } else if (parsingRequestLineQPos != -1 && !httpParser.isQueryRelaxed(chr)) {
                    //请求参数字符只能含：!$%&'()*+,-./:;=?@~_0-9A-Za-z
                    request.protocol().setString(Constants.HTTP_11);
                    String invalidRequestTarget = parseInvalid(parsingRequestLineStart, byteBuffer);
                    throw new IllegalArgumentException(sm.getString("iib.invalidRequestTarget", invalidRequestTarget));
                } else if (httpParser.isNotRequestTargetRelaxed(chr)) {
                    //当前字符不是：$%&'()*+,-./0-9:;=?@A-Z[]_a-z~
                    request.protocol().setString(Constants.HTTP_11);
                    String invalidRequestTarget = parseInvalid(parsingRequestLineStart, byteBuffer);
                    throw new IllegalArgumentException(sm.getString("iib.invalidRequestTarget", invalidRequestTarget));
                }
            }

            if (parsingRequestLineQPos >= 0) {
                //含请求参数，设置request.queryMB的值
                //请求参数若是URL编码结果，会在后面进行URL解码
                request.queryString().setBytes(byteBuffer.array(), parsingRequestLineQPos + 1,
                        end - parsingRequestLineQPos - 1);

                //设置request.uriMB的值
                request.requestURI().setBytes(byteBuffer.array(), parsingRequestLineStart,
                        parsingRequestLineQPos - parsingRequestLineStart);
            } else {
                //如果没带请求参数，则仅设置request.uriMB的值
                request.requestURI().setBytes(byteBuffer.array(), parsingRequestLineStart,
                        end - parsingRequestLineStart);
            }
            // HTTP/0.9 processing jumps to stage 7.
            // Don't want to overwrite that here.
            if (parsingRequestLinePhase == 4) {
                parsingRequestLinePhase = 5;
            }
        }

        //5、解析请求行的[协议及版本]部分前的准备，即：跳过请求行中[URI]后紧接着的所有空格、制表符
        if (parsingRequestLinePhase == 5) {
            boolean space = true;

            while (space) {
                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (!fill(false)) {
                        return false;
                    }
                }
                byte chr = byteBuffer.get();
                if (!(chr == Constants.SP || chr == Constants.HT)) {
                    space = false;
                    byteBuffer.position(byteBuffer.position() - 1);
                }
            }

            //记录请求行中协议部分的开始位置
            parsingRequestLineStart = byteBuffer.position();
            parsingRequestLinePhase = 6;
            end = 0;
        }

        //6、正式解析请求行的[协议及版本]部分，如：HTTP/1.1
        //解析协议及版本部分：内容只含[./0-9HPT]，不能含其他ascii码，直到遇到换行符为止
        if (parsingRequestLinePhase == 6) {

            //请求行读取到换行符为止
            while (!parsingRequestLineEol) {

                if (byteBuffer.position() >= byteBuffer.limit()) {
                    if (!fill(false)) {
                        return false;
                    }
                }

                int pos = byteBuffer.position();
                prevChr = chr;
                chr = byteBuffer.get();
                if (chr == Constants.CR) {
                    //跳过，校验后一个字节（可能是LF）
                } else if (prevChr == Constants.CR && chr == Constants.LF) {
                    // CRLF - 表面请求行解析结束
                    end = pos - 1;
                    // 请求行解析结束
                    parsingRequestLineEol = true;
                } else if (chr == Constants.LF) {
                    //LF - 可选的请求行解析结束
                    end = pos;
                    parsingRequestLineEol = true;
                } else if (prevChr == Constants.CR || !HttpParser.isHttpProtocol(chr)) {
                    //错误格式：
                    // 1、上个字节是CR，当前字节不是LF
                    // 2、当前字符不在 ./0-9HPT 中
                    String invalidProtocol = parseInvalid(parsingRequestLineStart, byteBuffer);
                    throw new IllegalArgumentException(sm.getString("iib.invalidHttpProtocol", invalidProtocol));
                }
            }

            //读取协议类型及版本
            if ((end - parsingRequestLineStart) > 0) {
                request.protocol().setBytes(byteBuffer.array(), parsingRequestLineStart,
                        end - parsingRequestLineStart);
                parsingRequestLinePhase = 7;
            }
        }

        //请求行解析完成
        if (parsingRequestLinePhase == 7) {
            parsingRequestLine = false;
            parsingRequestLinePhase = 0;
            parsingRequestLineEol = false;
            parsingRequestLineStart = 0;
            return true;
        }

        throw new IllegalStateException(sm.getString("iib.invalidPhase", Integer.valueOf(parsingRequestLinePhase)));
    }


    /**
     * 解析HTTP请求头部分
     * @return 是否解析请求头部分结束？ true - 解析结束，false - 尚未结束，需要等待剩余数据后继续解析
     */
    boolean parseHeaders() throws IOException {
        if (!parsingHeader) {
            throw new IllegalStateException(sm.getString("iib.parseheaders.ise.error"));
        }

        HeaderParseStatus status;

        do {
            //循环解析每一个header的name值和value值
            status = parseHeader();
            // 检查：
            // (1) 请求行部分 + 请求头部分  未超过指定上限
            // (2) byteBuffer缓冲区还有足够的空间来读取请求体
            if (byteBuffer.position() > headerBufferSize || byteBuffer.capacity() - byteBuffer.position() < socketReadBufferSize) {
                throw new IllegalArgumentException(sm.getString("iib.requestheadertoolarge.error"));
            }
        } while (status == HeaderParseStatus.HAVE_MORE_HEADERS);


        if (status == HeaderParseStatus.DONE) {
            //请求头已经解析完成
            parsingHeader = false;
            end = byteBuffer.position();
            return true;
        } else {
            //请求头尚未解析完成
            return false;
        }
    }


    int getParsingRequestLinePhase() {
        return parsingRequestLinePhase;
    }


    private String parseInvalid(int startPos, ByteBuffer buffer) {
        // Look for the next space
        byte b = 0;
        while (buffer.hasRemaining() && b != 0x20) {
            b = buffer.get();
        }
        String result = HeaderUtil.toPrintableString(buffer.array(), buffer.arrayOffset() + startPos, buffer.position() - startPos);
        if (b != 0x20) {
            // Ran out of buffer rather than found a space
            result = result + "...";
        }
        return result;
    }


    /**
     * End request (consumes leftover bytes).
     *
     * @throws IOException an underlying I/O error occurred
     */
    void endRequest() throws IOException {

        if (swallowInput && (lastActiveFilter != -1)) {
            int extraBytes = (int) activeFilters[lastActiveFilter].end();
            byteBuffer.position(byteBuffer.position() - extraBytes);
        }
    }


    @Override
    public int available() {
        return available(false);
    }


    /**
     * Available bytes in the buffers for the current request.
     *
     * Note that when requests are pipelined, the data in byteBuffer may relate
     * to the next request rather than this one.
     */
    int available(boolean read) {
        int available;

        if (lastActiveFilter == -1) {
            available = inputStreamInputBuffer.available();
        } else {
            available = activeFilters[lastActiveFilter].available();
        }

        // Only try a non-blocking read if:
        // - there is no data in the filters
        // - the caller requested a read
        // - there is no data in byteBuffer
        // - the socket wrapper indicates a read is allowed
        //
        // Notes: 1. When pipelined requests are being used available may be
        //        zero even when byteBuffer has data. This is because the data
        //        in byteBuffer is for the next request. We don't want to
        //        attempt a read in this case.
        //        2. wrapper.hasDataToRead() is present to handle the NIO2 case
        try {
            if (available == 0 && read && !byteBuffer.hasRemaining() && wrapper.hasDataToRead()) {
                fill(false);
                available = byteBuffer.remaining();
            }
        } catch (IOException ioe) {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("iib.available.readFail"), ioe);
            }
            // Not ideal. This will indicate that data is available which should
            // trigger a read which in turn will trigger another IOException and
            // that one can be thrown.
            available = 1;
        }
        return available;
    }


    /**
     * Has all of the request body been read? There are subtle differences
     * between this and available() &gt; 0 primarily because of having to handle
     * faking non-blocking reads with the blocking IO connector.
     */
    boolean isFinished() {
        // The active filters have the definitive information on whether or not
        // the current request body has been read. Note that byteBuffer may
        // contain pipelined data so is not a good indicator.
        if (lastActiveFilter >= 0) {
            return activeFilters[lastActiveFilter].isFinished();
        } else {
            // No filters. Assume request is not finished. EOF will signal end of
            // request.
            return false;
        }
    }

    ByteBuffer getLeftover() {
        int available = byteBuffer.remaining();
        if (available > 0) {
            return ByteBuffer.wrap(byteBuffer.array(), byteBuffer.position(), available);
        } else {
            return null;
        }
    }


    boolean isChunking() {
        for (int i = 0; i < lastActiveFilter; i++) {
            if (activeFilters[i] == filterLibrary[Constants.CHUNKED_FILTER]) {
                return true;
            }
        }
        return false;
    }


    void init(SocketWrapperBase<?> socketWrapper) {

        wrapper = socketWrapper;
        wrapper.setAppReadBufHandler(this);

        int bufLength = headerBufferSize +
                wrapper.getSocketBufferHandler().getReadBuffer().capacity();
        if (byteBuffer == null || byteBuffer.capacity() < bufLength) {
            byteBuffer = ByteBuffer.allocate(bufLength);
            byteBuffer.position(0).limit(0);
        }
    }



    // --------------------------------------------------------- Private Methods

    /**
     * 尝试从socket通道中读取数据，并放入byteBuffer中：
     *    若当前正在解析请求头（读取的是请求头数据），则缓存中的字节长度不能超值指定阈值[请求头字节长度有限制]
     *    若已经解析完请求头，需要设置byteBuffer中读取请求体的位置[后续读取的数据就是请求体的数据]
     * @param block  当没有从socket通道中读取到数据时是否需要阻塞等待数据到来？ true - 阻塞等待数据到来
     * @return 是否读取到数据？true - 读取到数据，false - 没有可读数据
     */
    private boolean fill(boolean block) throws IOException {

        if (log.isDebugEnabled()) {
            log.debug("Before fill(): parsingHeader: [" + parsingHeader +
                    "], parsingRequestLine: [" + parsingRequestLine +
                    "], parsingRequestLinePhase: [" + parsingRequestLinePhase +
                    "], parsingRequestLineStart: [" + parsingRequestLineStart +
                    "], byteBuffer.position(): [" + byteBuffer.position() +
                    "], byteBuffer.limit(): [" + byteBuffer.limit() +
                    "], end: [" + end + "]");
        }


        if (parsingHeader) {
            //请求行、请求头解析尚未完成...
            //请求头的数据大小不能超过上限[默认8KB]
            if (byteBuffer.limit() >= headerBufferSize) {
                if (parsingRequestLine) {
                    // Avoid unknown protocol triggering an additional error
                    request.protocol().setString(Constants.HTTP_11);
                }
                throw new IllegalArgumentException(sm.getString("iib.requestheadertoolarge.error"));
            }
        } else {
            //请求头解析完成，将byteBuffer的position、limit指向请求头数据结束的位置（请求头开始的位置）
            byteBuffer.limit(end).position(end);
        }

        int nRead;
        int mark = byteBuffer.position();
        try {

            if (byteBuffer.position() < byteBuffer.limit()) {
                byteBuffer.position(byteBuffer.limit());
            }
            byteBuffer.limit(byteBuffer.capacity());
            SocketWrapperBase<?> socketWrapper = this.wrapper;
            if (socketWrapper != null) {
                //从Socket读数据到byteBuffer中
                nRead = socketWrapper.read(block, byteBuffer);
            } else {
                throw new CloseNowException(sm.getString("iib.eof.error"));
            }
        } finally {
            // Ensure that the buffer limit and position are returned to a
            // consistent "ready for read" state if an error occurs during in
            // the above code block.
            // Some error conditions can result in the position being reset to
            // zero which also invalidates the mark.
            // https://bz.apache.org/bugzilla/show_bug.cgi?id=65677
            if (byteBuffer.position() >= mark) {
                // // Position and mark are consistent. Assume a read (possibly
                // of zero bytes) has occurred.
                byteBuffer.limit(byteBuffer.position());
                byteBuffer.position(mark);
            } else {
                // Position and mark are inconsistent. Set position and limit to
                // zero so effectively no data is reported as read.
                byteBuffer.position(0);
                byteBuffer.limit(0);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Received ["
                    + new String(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), StandardCharsets.ISO_8859_1) + "]");
        }

        if (nRead > 0) {
            return true;
        } else if (nRead == -1) {
            throw new EOFException(sm.getString("iib.eof.error"));
        } else {
            return false;
        }

    }


    /**
     * <pre>
     * 解析请求头中的键值对(name=value)，每次调用该方法只解析一个请求头
     *    1、请求行与请求头之间只能包含一个换行符
     *    2、请求头的header的name与value以冒号分割；name只能含ascii，且字符只能是小写；多个值用逗号隔开，如：Accept-Language: en-US, en;q=0.5
     *    3、header与header之间以换行符分割
     *    4、若请求行后面紧接2个换行符，表示请求头解析结束
     *    5、若一个header后面紧接2个换行符，表示请求头解析结束
     *    <b>注意： 1、读取到连续2个换行符表示请求头部分解析结束 </b>
     *    <b>      2、一个header可以包含跨行的value值（换行后另一行紧接着一个空格和制表符，或2个空格），如：
     *          |Content-Type: text/html;
     *          |  charset=UTF-8
     *    </b>
     *
     *
     * 返回的解析状态：
     *     1、HeaderParseStatus.NEED_MORE_DATA        数据不完整，需要等待请求头剩余数据到达
     *     2、HeaderParseStatus.DONE                  请求头部分已经解析完成（可能没有请求头数据）
     *     3、HeaderParsePosition.HEADER_SKIPLINE     跳过当前行
     *     4、HeaderParsePosition.HEADER_START        解析header的name前的准备，如：略过换行符，记录name的起始位置...
     *     5、HeaderParsePosition.HEADER_NAME         开始解析header的name（直到冒号为止）
     *     6、HeaderParsePosition.HEADER_VALUE_START  解析header的value前的准备，如：略过前面的空格，记录value的起始位置
     *     7、HeaderParsePosition.HEADER_VALUE        开始解析header的value（直到换行符为止）
     *     8、HeaderParsePosition.HEADER_MULTI_LINE   header可能含多个value值，且value值可能是跨行的（跨行的值通常需要连续2个空格开头）
     *     9、HeaderParseStatus.HAVE_MORE_HEADERS     继续解析下一个header
     * </pre>
     */
    private HeaderParseStatus parseHeader() throws IOException {

        //** 注意：请求行部分与请求头部分之间[仅包含一个换行符]  **

        //尝试解析请求头部分：
        // 1、请求行和请求头之间只含一个换行符，若有连续2个换行符表示请求头部分没有数据，终止请求头解析
        // 2、请求行与请求头，请求头的header与header之间都只含有一个换行符，略过换行符
        while (headerParsePos == HeaderParsePosition.HEADER_START) {

            if (byteBuffer.position() >= byteBuffer.limit()) {
                if (!fill(false)) {
                    return HeaderParseStatus.NEED_MORE_DATA;
                }
            }

            //请求行解析完后，prevChr = chr = LF
            prevChr = chr;
            chr = byteBuffer.get();

            if (chr == Constants.CR && prevChr != Constants.CR) {
                // prevChr=LF（请求行结束位置的换行符），chr=CR（紧接着的还是换行符？）
                //若下一个字节是LF，则有连续2个换行符，表示没有请求头部分
            } else if (chr == Constants.LF) {
                //解析到换行符（请求行后面的换行符后紧接着还有一个换行符），表示没有请求头部分，终止请求头部分解析
                return HeaderParseStatus.DONE;
            } else {
                //没有换行符，需要解析请求头
                if (prevChr == Constants.CR) {
                    //前一个字节是CR，第二个字节不是LF，不算换行符，回退2个位置（当前字节和前一个位置CR）
                    byteBuffer.position(byteBuffer.position() - 2);
                } else {
                    //回退一个位置（当前字节）
                    byteBuffer.position(byteBuffer.position() - 1);
                }
                break;
            }
        }

        //2、标记好缓冲区中解析请求头数据的起始位置，下一步开始解析第一个header
        if (headerParsePos == HeaderParsePosition.HEADER_START) {
            //缓冲区中请求头数据的开始位置
            headerData.start = byteBuffer.position();
            headerData.lineStart = headerData.start;
            //下一步开始解析header的name
            headerParsePos = HeaderParsePosition.HEADER_NAME;
        }


        //** 解析请求头中的name（名称必须是US-ASCII，name若包含字母必须小写）：
        // 每一个header的name和value以冒号分割（:），header与header之间以分号隔开（;）
        // 所有header中的name含有的字符必须是小写
        // 每一个header独占一行
        while (headerParsePos == HeaderParsePosition.HEADER_NAME) {

            if (byteBuffer.position() >= byteBuffer.limit()) {
                if (!fill(false)) {
                    return HeaderParseStatus.NEED_MORE_DATA;
                }
            }

            int pos = byteBuffer.position();
            chr = byteBuffer.get();
            //COLON - 冒号(:)
            if (chr == Constants.COLON) {
                //header的name已解析完成，遇到冒号，下一步开始解析value值
                headerParsePos = HeaderParsePosition.HEADER_VALUE_START;

                //添加header的name到headers中
                headerData.headerValue = headers.addValue(byteBuffer.array(), headerData.start,
                        pos - headerData.start);
                pos = byteBuffer.position();
                //记录value的起始位置
                headerData.start = pos;
                headerData.realPos = pos;
                headerData.lastSignificantChar = pos;
                break;
            } else if (!HttpParser.isToken(chr)) {
                //若header的name部分含有非法字符时：
                //  当rejectIllegalHeader=false时，忽略当前解析的行，从新从下一行开始解析请求头
                //  当rejectIllegalHeader=true时，直接抛异常并终止整个请求
                headerData.lastSignificantChar = pos;
                byteBuffer.position(byteBuffer.position() - 1);
                return skipLine();
            }

            //header的name值中的字符若是大写，需转为小写
            if ((chr >= Constants.A) && (chr <= Constants.Z)) {
                byteBuffer.put(pos, (byte) (chr - Constants.LC_OFFSET));
            }
        }

        if (headerParsePos == HeaderParsePosition.HEADER_SKIPLINE) {
            //忽略当前行
            return skipLine();
        }

       //** 解析请求头中的name对应的value（value的值可能是单个值、多个值）
        while (headerParsePos == HeaderParsePosition.HEADER_VALUE_START ||
               headerParsePos == HeaderParsePosition.HEADER_VALUE ||
               headerParsePos == HeaderParsePosition.HEADER_MULTI_LINE) {

            //跳过value前的所有空格、制表符
            if (headerParsePos == HeaderParsePosition.HEADER_VALUE_START) {
                while (true) {

                    if (byteBuffer.position() >= byteBuffer.limit()) {
                        if (!fill(false)) {
                            return HeaderParseStatus.NEED_MORE_DATA;
                        }
                    }

                    chr = byteBuffer.get();
                    if (!(chr == Constants.SP || chr == Constants.HT)) {
                        headerParsePos = HeaderParsePosition.HEADER_VALUE;
                        byteBuffer.position(byteBuffer.position() - 1);
                        break;
                    }
                }
            }

            //记录当前header行的value的结束位置（读到当前header末尾的第一个换行符截止）
            if (headerParsePos == HeaderParsePosition.HEADER_VALUE) {
                boolean eol = false;
                while (!eol) {

                    if (byteBuffer.position() >= byteBuffer.limit()) {
                        if (!fill(false)) {
                            return HeaderParseStatus.NEED_MORE_DATA;
                        }
                    }

                    prevChr = chr;
                    chr = byteBuffer.get();
                    if (chr == Constants.CR) {
                        //预测是CR+LF，继续读取下个字节进行判断
                    } else if (chr == Constants.LF) {
                        // 如果是CR+LF或者是LF，则表示已经读取到当前value值的结尾，终止读取...
                        eol = true;
                    } else if (prevChr == Constants.CR) {
                        //出现单独CR（没有LF，非法数据），从headers中移除本次的header
                        headers.removeHeader(headers.size() - 1);
                        return skipLine();
                    } else if (chr != Constants.HT && HttpParser.isControl(chr)) {
                        //非法字符（0-31共32个控制字符，127 DEL控制字符），从headers中移除当前header
                        headers.removeHeader(headers.size() - 1);
                        return skipLine();
                    } else if (chr == Constants.SP || chr == Constants.HT) {
                        byteBuffer.put(headerData.realPos, chr);
                        headerData.realPos++;
                    } else {
                        byteBuffer.put(headerData.realPos, chr);
                        headerData.realPos++;
                        headerData.lastSignificantChar = headerData.realPos;
                    }
                }

                //记录当前header的value值的终止位置
                headerData.realPos = headerData.lastSignificantChar;
                //当前header的value值可能有多个值，且值跨行
                headerParsePos = HeaderParsePosition.HEADER_MULTI_LINE;
            }


            //这里继续从socket缓冲区读数据的作用是：
            // 1、当前header已经解析完（包括这一行结尾的换行符），但要获取下一个header
            // 2、当前header已经解析完（包括这一行结尾的换行符），没有下一个header，但要继续读一个换行符表示请求头部分解析完成
            if (byteBuffer.position() >= byteBuffer.limit()) {
                if (!fill(false)) {
                    return HeaderParseStatus.NEED_MORE_DATA;
                }
            }

            //若换行符后面还有换行符，则终止请求头解析
            byte peek = byteBuffer.get(byteBuffer.position());
            if (headerParsePos == HeaderParsePosition.HEADER_MULTI_LINE) {
                //尝试继续解析下一个header
                if ((peek != Constants.SP) && (peek != Constants.HT)) {
                    headerParsePos = HeaderParsePosition.HEADER_START;
                    break;
                } else {
                    //跨行的value值前面需有2个空格
                    byteBuffer.put(headerData.realPos, peek);
                    headerData.realPos++;
                    headerParsePos = HeaderParsePosition.HEADER_VALUE_START;
                }
            }
        }

        //设置当前header的value值
        headerData.headerValue.setBytes(byteBuffer.array(), headerData.start,
                headerData.lastSignificantChar - headerData.start);
        headerData.recycle();
        return HeaderParseStatus.HAVE_MORE_HEADERS;
    }


    private HeaderParseStatus skipLine() throws IOException {
        headerParsePos = HeaderParsePosition.HEADER_SKIPLINE;
        boolean eol = false;

        // Reading bytes until the end of the line
        while (!eol) {

            // Read new bytes if needed
            if (byteBuffer.position() >= byteBuffer.limit()) {
                if (!fill(false)) {
                    return HeaderParseStatus.NEED_MORE_DATA;
                }
            }

            int pos = byteBuffer.position();
            prevChr = chr;
            chr = byteBuffer.get();
            if (chr == Constants.CR) {
                // Skip
            } else if (chr == Constants.LF) {
                // CRLF or LF is an acceptable line terminator
                eol = true;
            } else {
                headerData.lastSignificantChar = pos;
            }
        }
        if (rejectIllegalHeader || log.isDebugEnabled()) {
            String message = sm.getString("iib.invalidheader",
                    HeaderUtil.toPrintableString(byteBuffer.array(), headerData.lineStart,
                            headerData.lastSignificantChar - headerData.lineStart + 1));
            if (rejectIllegalHeader) {
                throw new IllegalArgumentException(message);
            }
            log.debug(message);
        }

        headerParsePos = HeaderParsePosition.HEADER_START;
        return HeaderParseStatus.HAVE_MORE_HEADERS;
    }


    // ----------------------------------------------------------- Inner classes

    private enum HeaderParseStatus {
        DONE, HAVE_MORE_HEADERS, NEED_MORE_DATA
    }


    private enum HeaderParsePosition {
        /**
         * Start of a new header. A CRLF here means that there are no more
         * headers. Any other character starts a header name.
         */
        HEADER_START,
        /**
         * Reading a header name. All characters of header are HTTP_TOKEN_CHAR.
         * Header name is followed by ':'. No whitespace is allowed.<br>
         * Any non-HTTP_TOKEN_CHAR (this includes any whitespace) encountered
         * before ':' will result in the whole line being ignored.
         */
        HEADER_NAME,
        /**
         * Skipping whitespace before text of header value starts, either on the
         * first line of header value (just after ':') or on subsequent lines
         * when it is known that subsequent line starts with SP or HT.
         */
        HEADER_VALUE_START,
        /**
         * Reading the header value. We are inside the value. Either on the
         * first line or on any subsequent line. We come into this state from
         * HEADER_VALUE_START after the first non-SP/non-HT byte is encountered
         * on the line.
         */
        HEADER_VALUE,
        /**
         * Before reading a new line of a header. Once the next byte is peeked,
         * the state changes without advancing our position. The state becomes
         * either HEADER_VALUE_START (if that first byte is SP or HT), or
         * HEADER_START (otherwise).
         */
        HEADER_MULTI_LINE,
        /**
         * Reading all bytes until the next CRLF. The line is being ignored.
         */
        HEADER_SKIPLINE
    }


    private static class HeaderParseData {
        /**
         * The first character of the header line.
         */
        int lineStart = 0;
        /**
         * When parsing header name: first character of the header.<br>
         * When skipping broken header line: first character of the header.<br>
         * When parsing header value: first character after ':'.
         */
        int start = 0;
        /**
         * When parsing header name: not used (stays as 0).<br>
         * When skipping broken header line: not used (stays as 0).<br>
         * When parsing header value: starts as the first character after ':'.
         * Then is increased as far as more bytes of the header are harvested.
         * Bytes from buf[pos] are copied to buf[realPos]. Thus the string from
         * [start] to [realPos-1] is the prepared value of the header, with
         * whitespaces removed as needed.<br>
         */
        int realPos = 0;
        /**
         * When parsing header name: not used (stays as 0).<br>
         * When skipping broken header line: last non-CR/non-LF character.<br>
         * When parsing header value: position after the last not-LWS character.<br>
         */
        int lastSignificantChar = 0;
        /**
         * MB that will store the value of the header. It is null while parsing
         * header name and is created after the name has been parsed.
         */
        MessageBytes headerValue = null;
        public void recycle() {
            lineStart = 0;
            start = 0;
            realPos = 0;
            lastSignificantChar = 0;
            headerValue = null;
        }
    }


    // ------------------------------------- InputStreamInputBuffer Inner Class

    /**
     * This class is an input buffer which will read its data from an input
     * stream.
     */
    private class SocketInputBuffer implements InputBuffer {

        @Override
        public int doRead(ApplicationBufferHandler handler) throws IOException {

            if (byteBuffer.position() >= byteBuffer.limit()) {
                // The application is reading the HTTP request body
                boolean block = (request.getReadListener() == null);
                if (!fill(block)) {
                    if (block) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            }

            int length = byteBuffer.remaining();
            handler.setByteBuffer(byteBuffer.duplicate());
            byteBuffer.position(byteBuffer.limit());

            return length;
        }

        @Override
        public int available() {
            return byteBuffer.remaining();
        }
    }


    @Override
    public void setByteBuffer(ByteBuffer buffer) {
        byteBuffer = buffer;
    }


    @Override
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }


    @Override
    public void expand(int size) {
        if (byteBuffer.capacity() >= size) {
            byteBuffer.limit(size);
        }
        ByteBuffer temp = ByteBuffer.allocate(size);
        temp.put(byteBuffer);
        byteBuffer = temp;
        byteBuffer.mark();
        temp = null;
    }
}
