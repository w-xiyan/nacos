/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.naming.healthcheck.heartbeat.BeatProcessor;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Thread to update ephemeral instance triggered by client beat for v1.x.
 *
 * @author nkorange
 */
public class ClientBeatProcessor implements BeatProcessor {
    
    private RsInfo rsInfo;
    
    private Service service;
    
    @JsonIgnore
    public UdpPushService getPushService() {
        return ApplicationUtils.getBean(UdpPushService.class);
    }
    
    public RsInfo getRsInfo() {
        return rsInfo;
    }
    
    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }
    
    public Service getService() {
        return service;
    }
    
    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public void run() {
        Service service = this.service;
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }
        // IP
        String ip = rsInfo.getIp();
        // 集群名字
        String clusterName = rsInfo.getCluster();
        // 端口
        int port = rsInfo.getPort();
        // 获取集群
        Cluster cluster = service.getClusterMap().get(clusterName);
        // 获取集群所有的临时服务实例
        List<Instance> instances = cluster.allIPs(true);

//    遍历所有的实例，刷新lastBeat的时间。如果没有被标记并且健康状态不对，则设置健康状态为true，同时发送ServiceChangeEvent事件。
        // 遍历更新对应的状态
        for (Instance instance : instances) {
            // 如果IP和端口一致则更新
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                //刷新心跳时间
                instance.setLastBeat(System.currentTimeMillis());
                //没被标记的&不健康的
                if (!instance.isMarked() && !instance.isHealthy()) {
                    //设置为健康
                    instance.setHealthy(true);
                    Loggers.EVT_LOG
                            .info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                                    cluster.getService().getName(), ip, port, cluster.getName(),
                                    UtilsAndCommons.LOCALHOST_SITE);
                    //发送服务改变事件，PushService实现了 ApplicationListener<ServiceChangeEvent>，事件监听时发生UDP消息通知
                    getPushService().serviceChanged(service);
                }
            }
        }
    }
}
