/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.runner;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.apache.eagle.alert.utils.AlertConstants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class AlertPublisherBoltTest {

    @Test
    public void testNormal() throws Exception {
        IMetadataChangeNotifyService notifyService = Mockito.mock(IMetadataChangeNotifyService.class);
        Config config = Mockito.mock(Config.class);
        TopologyContext context = Mockito.mock(TopologyContext.class);
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        AlertPublisherBolt alertPublisherBolt = new AlertPublisherBolt("testPublisherName",
            config,
            notifyService);
        Mockito.when(context.registerMetric(
            Mockito.any(String.class),
            Mockito.any(MultiCountMetric.class),
            Mockito.any(Integer.class)))
            .thenReturn(new MultiCountMetric());
        alertPublisherBolt.prepare(Maps.newHashMap(), context, collector);

        Tuple tuple = Mockito.mock(Tuple.class);
        PublishPartition publishPartition = Mockito.mock(PublishPartition.class);
        Mockito.when(tuple.getValueByField(Mockito.eq(AlertConstants.FIELD_0))).thenReturn(publishPartition);

        String policyId = "nerveCenter_port_down";
        String streamId = "ncAlertOutputStream";
        Map<String, StreamDefinition> streamDefinitionMap = Maps.newHashMap();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/publisher/testStream.json"), StreamDefinition.class);
        streamDefinitionMap.put(streamId, streamDefinition);
        Map<String, PolicyDefinition> policyDefinitionMap = Maps.newHashMap();
        PolicyDefinition policyDefinition = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPolicy.json"), PolicyDefinition.class);
        policyDefinitionMap.put(policyId, policyDefinition);
        Publishment publishment = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPublisher.json"), Publishment.class);
        PublishSpec publishSpec = new PublishSpec("testTopo", "testBolt");
        publishSpec.addPublishment(publishment);

        alertPublisherBolt.onAlertPolicyChange(policyDefinitionMap, streamDefinitionMap);
        alertPublisherBolt.onAlertPublishSpecChange(publishSpec, streamDefinitionMap);

        Mockito.when(publishPartition.getPolicyId()).thenReturn(policyId);
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();
        alertStreamEvent.setPolicyId(policyId);
        alertStreamEvent.setSchema(streamDefinition);
        alertStreamEvent.setTimestamp(System.currentTimeMillis());
        alertStreamEvent.setData(new Object[streamDefinition.getColumns().size()]);
        // TODO initialize event
        Mockito.when(tuple.getValueByField(Mockito.eq(AlertConstants.FIELD_1))).thenReturn(alertStreamEvent);
        collector.ack(Mockito.any(Tuple.class));
        alertPublisherBolt.execute(tuple);

        Map<String, PolicyDefinition> policyDefinitionMap2 = Maps.newHashMap();
        PolicyDefinition policyDefinition2 = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPolicy2.json"), PolicyDefinition.class);
        Optional.ofNullable(policyDefinition2)
            .filter(pd -> pd != null
                && pd.getActiveSuppressEvent() != null
                && pd.getActiveSuppressEvent().getDuration() != null)
            .ifPresent(pd -> pd.getActiveSuppressEvent().setExpireTime(
                new Date(System.currentTimeMillis() +
                    org.joda.time.Period.parse(
                        pd.getActiveSuppressEvent().getDuration())
                        .toStandardSeconds().getSeconds() * 1000)));
        policyDefinitionMap2.put(policyId, policyDefinition2);

        alertStreamEvent.getData()[streamDefinition.getColumnIndex("severityCode")] = 2;
        alertStreamEvent.getData()[streamDefinition.getColumnIndex("name")] = "nerveCenterAlert";

        alertPublisherBolt.onAlertPolicyChange(policyDefinitionMap2, streamDefinitionMap);
        alertPublisherBolt.execute(tuple);

        AlertStreamEvent event = (AlertStreamEvent) tuple.getValueByField(AlertConstants.FIELD_1);
        Assert.assertTrue(event.getContext().containsKey("pause"));
        Assert.assertTrue((Boolean) event.getContext().get("pause"));
    }

    @Test
    public void testExceptional() throws Exception {
        IMetadataChangeNotifyService notifyService = Mockito.mock(IMetadataChangeNotifyService.class);
        Config config = Mockito.mock(Config.class);
        TopologyContext context = Mockito.mock(TopologyContext.class);
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        AlertPublisherBolt alertPublisherBolt = new AlertPublisherBolt("testPublisherName",
            config,
            notifyService);
        Mockito.when(context.registerMetric(
            Mockito.any(String.class),
            Mockito.any(MultiCountMetric.class),
            Mockito.any(Integer.class)))
            .thenReturn(new MultiCountMetric());
        alertPublisherBolt.prepare(Maps.newHashMap(), context, collector);

        Tuple tuple = Mockito.mock(Tuple.class);
        PublishPartition publishPartition = Mockito.mock(PublishPartition.class);
        Mockito.when(tuple.getValueByField(Mockito.eq(AlertConstants.FIELD_0))).thenReturn(publishPartition);

        String policyId = "nerveCenter_port_down";
        String streamId = "ncAlertOutputStream";
        Map<String, StreamDefinition> streamDefinitionMap = Maps.newHashMap();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/publisher/testStream.json"), StreamDefinition.class);
        streamDefinitionMap.put(streamId, streamDefinition);
        Map<String, PolicyDefinition> policyDefinitionMap = Maps.newHashMap();
        PolicyDefinition policyDefinition = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPolicy.json"), PolicyDefinition.class);
        policyDefinitionMap.put(policyId, policyDefinition);
        Publishment publishment = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPublisher.json"), Publishment.class);
        PublishSpec publishSpec = new PublishSpec("testTopo", "testBolt");
        publishSpec.addPublishment(publishment);

        alertPublisherBolt.onAlertPolicyChange(policyDefinitionMap, streamDefinitionMap);
        alertPublisherBolt.onAlertPublishSpecChange(publishSpec, streamDefinitionMap);

        Mockito.when(publishPartition.getPolicyId()).thenReturn(policyId);
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();
        alertStreamEvent.setPolicyId(policyId);
        alertStreamEvent.setSchema(streamDefinition);
        alertStreamEvent.setTimestamp(System.currentTimeMillis());
        alertStreamEvent.setData(new Object[streamDefinition.getColumns().size()]);
        // TODO initialize event
        Mockito.when(tuple.getValueByField(Mockito.eq(AlertConstants.FIELD_1))).thenReturn(alertStreamEvent);
        collector.ack(Mockito.any(Tuple.class));

        Map<String, PolicyDefinition> policyDefinitionMap2 = Maps.newHashMap();
        PolicyDefinition policyDefinition2 = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/publisher/testPolicy2.json"), PolicyDefinition.class);
        Optional.ofNullable(policyDefinition2)
            .filter(pd -> pd != null
                && pd.getActiveSuppressEvent() != null
                && pd.getActiveSuppressEvent().getDuration() != null)
            .ifPresent(pd -> pd.getActiveSuppressEvent().setExpireTime(
                new Date(System.currentTimeMillis() +
                    org.joda.time.Period.parse(
                        pd.getActiveSuppressEvent().getDuration())
                        .toStandardSeconds().getSeconds() * 1000)));
        policyDefinitionMap2.put(policyId, policyDefinition2);

        alertStreamEvent.getData()[streamDefinition.getColumnIndex("severityCode")] = 6;
        alertStreamEvent.getData()[streamDefinition.getColumnIndex("name")] = "nerveCenterAlert";

        alertPublisherBolt.onAlertPolicyChange(policyDefinitionMap2, streamDefinitionMap);
        alertPublisherBolt.execute(tuple);

        AlertStreamEvent event = (AlertStreamEvent) tuple.getValueByField(AlertConstants.FIELD_1);
        Assert.assertFalse(event.getContext().containsKey("pause"));
    }

}
