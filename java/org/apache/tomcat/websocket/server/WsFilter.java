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
package org.apache.tomcat.websocket.server;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handles the initial HTTP connection for WebSocket connections.
 */
public class WsFilter extends GenericFilter {

    private static final long serialVersionUID = 1L;

    private transient WsServerContainer sc;


    @Override
    public void init() throws ServletException {
        sc = (WsServerContainer) getServletContext().getAttribute(
                Constants.SERVER_CONTAINER_SERVLET_CONTEXT_ATTRIBUTE);
    }


    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {

        // 1、WsServerContainer中有注册了WsServerEndpoint；
        // 2、并且请求方式为GET，请求头中含Upgrade=websocket；
        // 以上2个条件都满足，将进行websocket升级处理....
        if (!sc.areEndpointsRegistered() ||
                !UpgradeUtil.isWebSocketUpgradeRequest(request, response)) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        // 确认请求路径是否匹配websocket的mapping....
        String path;
        String pathInfo = req.getPathInfo();
        if (pathInfo == null) {
            path = req.getServletPath();
        } else {
            path = req.getServletPath() + pathInfo;
        }

        //根据请求路径获取websocket的mapping
        WsMappingResult mappingResult = sc.findMapping(path);

        if (mappingResult == null) {
            //已注册的websocket服务端点没有与当前的请求路径匹配成功，则不当做websocket处理....
            chain.doFilter(request, response);
            return;
        }

        //当前http/https请求是websocket升级请求，开始升级为websocket协议并处理连接...
        UpgradeUtil.doUpgrade(sc, req, resp, mappingResult.getConfig(),
                mappingResult.getPathParams());
    }
}
