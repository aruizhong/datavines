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

import io.datavines.common.utils.HttpUtils;
import io.datavines.common.utils.JSONUtils;
import io.datavines.notification.api.entity.SlaNotificationResultRecord;
import io.datavines.notification.api.entity.SlaSenderMessage;
import io.datavines.notification.plugin.oncall.entity.ReceiverConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@EqualsAndHashCode
@Data
public class OnCallSender {

    private String messageKey = "Datavines告警";

    public OnCallSender(SlaSenderMessage senderMessage) {

    }

    public SlaNotificationResultRecord sendMsg(Set<ReceiverConfig> receiverSet, String subject, String message) {
        SlaNotificationResultRecord result = new SlaNotificationResultRecord();
        // if there is no receivers , no need to process
        if (CollectionUtils.isEmpty(receiverSet)) {
            return result;
        }
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        // send
        Set<ReceiverConfig> failToReceivers = new HashSet<>();
        for (ReceiverConfig receiverConfig : receiverSet) {
            try {
                String webhookURL = receiverConfig.getWebhookURL();
                String title = receiverConfig.getTitle();
                HashMap<String, String> msgMap = new HashMap<>();
                msgMap.put("title", title);
                msgMap.put("message", joinMsg(title, message));
                String msgStr = JSONUtils.toJsonString(msgMap);
                log.info("time:{}, message:{}" , LocalDateTime.now(), msgStr);
                HashMap<String, String> headers = new HashMap<>();
                headers.put("content-type", "application/json;charset=UTF-8");
                String ret = HttpUtils.post(webhookURL, msgStr, headers);
            } catch (Exception e) {
                failToReceivers.add(receiverConfig);
                log.error("OnCall send error", e);
            }
        }

        if (!CollectionUtils.isEmpty(failToReceivers)) {
            String recordMessage = String.format("send to %s fail", failToReceivers.stream().map(ReceiverConfig::getTitle).collect(Collectors.joining(",")));
            result.setStatus(false);
            result.setMessage(recordMessage);
        } else {
            result.setStatus(true);
        }
        return result;
    }

    private String joinMsg(String title, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("**%s-%s**", messageKey, title));
        String str = message.replace("\"", "");
        for (String line : str.substring(1, str.length() - 1).split(",")) {
            sb.append("\n").append(line);
        }
        return sb.toString();
    }
}
