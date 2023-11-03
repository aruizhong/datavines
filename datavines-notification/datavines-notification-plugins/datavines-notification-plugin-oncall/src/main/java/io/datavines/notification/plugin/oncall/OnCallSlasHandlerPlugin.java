/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.notification.plugin.oncall;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.common.param.form.PluginParams;
import io.datavines.common.param.form.Validate;
import io.datavines.common.param.form.type.InputParam;
import io.datavines.common.utils.JSONUtils;
import io.datavines.notification.api.entity.*;
import io.datavines.notification.api.spi.SlasHandlerPlugin;
import io.datavines.notification.plugin.oncall.entity.ReceiverConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class OnCallSlasHandlerPlugin implements SlasHandlerPlugin {

    /**
     * notify by OnCall
     * @param config In config, there are not only email channels, but also channels such as Feishu. SlaSenderMessage represents each channel, and then Set<SlaConfigMessage> represents the set of people sent to A and B
     *               config里面不仅是邮箱途径，还会是飞书等途径，SlaSenderMessage就表示各个途径，然后Set<SlaConfigMessage>表示发送给A这批人，发送给B这批人的集合
     * @return
     */
    @Override
    public SlaNotificationResult notify(SlaNotificationMessage slaNotificationMessage, Map<SlaSenderMessage, Set<SlaConfigMessage>> config) {
        Set<SlaSenderMessage> onCallSenderSet = config.keySet().stream().filter(x -> "OnCall".equals(x.getType())).collect(Collectors.toSet());
        SlaNotificationResult result = new SlaNotificationResult();
        ArrayList<SlaNotificationResultRecord> records = new ArrayList<>();
        result.setStatus(true);
        String subject = slaNotificationMessage.getSubject();
        String message = slaNotificationMessage.getMessage();
        // Start looping each alarm channel. 开始循环每个告警途径。
        for (SlaSenderMessage senderMessage : onCallSenderSet) {
            OnCallSender onCallSender = new OnCallSender(senderMessage);
            Set<SlaConfigMessage> slaConfigMessageSet = config.get(senderMessage);
            HashSet<ReceiverConfig> toReceivers = new HashSet<>();
            // At the beginning of the loop, which group of people should be sent? Place the notifier in the same list and issue a unified alarm in the future. 开始循环要发送给哪组人；把通知人放到同一个list里，后续统一告警出去。
            for (SlaConfigMessage receiver: slaConfigMessageSet) {
                String receiverConfigStr = receiver.getConfig();
                ReceiverConfig receiverConfig = JSONUtils.parseObject(receiverConfigStr, ReceiverConfig.class);
                toReceivers.add(receiverConfig);
            }

            SlaNotificationResultRecord record = onCallSender.sendMsg(toReceivers, subject, message);
            if (record.getStatus().equals(false)) {
                record.setMessage(record.getMessage());
                result.setStatus(false);
            }
            records.add(record);
        }
        result.setRecords(records);
        return result;
    }

    /**
     *
     * The alarm channel has unchanged configuration, such as the server configuration of the email channel. 告警途径，不变的配置，比如邮箱途径的服务器配置是不变的配置。
     * @return
     */
    @Override
    public String getConfigSenderJson() {
        return "Nothing";
    }

    @Override
    public String getConfigJson() {

        List<PluginParams> paramsList = new ArrayList<>();

        InputParam title = InputParam.newBuilder("title", "title")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        InputParam webhookURL = InputParam.newBuilder("webhookURL", "webhookURL")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        paramsList.add(title);
        paramsList.add(webhookURL);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(paramsList);
        } catch (JsonProcessingException e) {
            log.error("json parse error : {}", e.getMessage(), e);
        }

        return result;
    }
}
