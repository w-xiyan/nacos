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

package com.alibaba.nacos.example;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Config service example.
 *
 * @author Nacos
 */
public class ConfigExample {

    public static void main(String[] args) throws NacosException, InterruptedException {
        //1.构建配置Properties对象，添加serverAddr
        String serverAddr = "localhost";
        String dataId = "test";
        String group = "DEFAULT_GROUP";
        Properties properties = new Properties();
        properties.put("serverAddr", serverAddr);

        //2.创建ConfigService实例，这个是nacos作为配置中心时的客户端接口。
        ConfigService configService = NacosFactory.createConfigService(properties);
        //3.根据dataId和group获取配置内容。 配置资源的资源坐标由dataId、group、namespace唯一确定。configService在初始化时给了默认的namespace。
        String content = configService.getConfig(dataId, group, 5000);
        System.out.println(content);
        //4.添加监听器
        configService.addListener(dataId, group, new Listener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("receive:" + configInfo);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });

        //发布、获取、删除配置等操作
        boolean isPublishOk = configService.publishConfig(dataId, group, "content");
        System.out.println(isPublishOk);

        Thread.sleep(3000);
        content = configService.getConfig(dataId, group, 5000);
        System.out.println(content);

        boolean isRemoveOk = configService.removeConfig(dataId, group);
        System.out.println(isRemoveOk);
        Thread.sleep(3000);

        content = configService.getConfig(dataId, group, 5000);
        System.out.println(content);
        Thread.sleep(300000);

    }
}
