/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.core.utils.Loggers;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Nacos based request handler.
 *
 * @author liuzunfei
 * @author xiweng.yy
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class RequestHandler<T extends Request, S extends Response> {
    /**
     * com.alibaba.nacos.config.server.remote.ConfigChangeBatchListenRequestHandler 	节点之间配置互相同步的处理器
     * com.alibaba.nacos.config.server.remote.ConfigChangeBatchListenRequestHandler 	配置改变监听处理器
     * com.alibaba.nacos.config.server.remote.ConfigPublishRequestHandler 	配置发布监听处理器
     * com.alibaba.nacos.config.server.remote.ConfigQueryRequestHandler 	配置查询请求处理器
     * com.alibaba.nacos.config.server.remote.ConfigRemoveRequestHandler 	配置移除请求处理器
     * com.alibaba.nacos.naming.remote.rpc.handler.DistroDataRequestHandler 	distro一致性服务处理器（节点点同步数据）
     * com.alibaba.nacos.core.remote.HealthCheckRequestHandler 	健康检查处理器
     * com.alibaba.nacos.naming.remote.rpc.handler.InstanceRequestHandler 	实例注册，移除处理器
     * com.alibaba.nacos.core.remote.core.ServerLoaderInfoRequestHandler 	服务信息加载处理器
     * com.alibaba.nacos.naming.remote.rpc.handler.ServiceListRequestHandler 	服务列表请求处理器
     * com.alibaba.nacos.naming.remote.rpc.handler.ServiceQueryRequestHandler 	服务查询处理器
     */
    @Autowired
    private RequestFilters requestFilters;
    
    /**
     * Handler request.
     *
     * @param request request
     * @param meta    request meta data
     * @return response
     * @throws NacosException nacos exception when handle request has problem.
     */
    public Response handleRequest(T request, RequestMeta meta) throws NacosException {
        for (AbstractRequestFilter filter : requestFilters.filters) {
            try {
                Response filterResult = filter.filter(request, meta, this.getClass());
                if (filterResult != null && !filterResult.isSuccess()) {
                    return filterResult;
                }
            } catch (Throwable throwable) {
                Loggers.REMOTE.error("filter error", throwable);
            }
            
        }
        return handle(request, meta);
    }
    
    /**
     * Handler request.
     *
     * @param request request
     * @param meta    request meta data
     * @return response
     * @throws NacosException nacos exception when handle request has problem.
     */
    public abstract S handle(T request, RequestMeta meta) throws NacosException;
    
}
