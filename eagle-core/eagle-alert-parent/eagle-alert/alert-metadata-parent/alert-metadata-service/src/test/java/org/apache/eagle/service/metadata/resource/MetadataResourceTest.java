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
package org.apache.eagle.service.metadata.resource;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.SuppressEvent;
import org.apache.eagle.alert.engine.coordinator.SuppressType;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;

public class MetadataResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataResourceTest.class);

    @Test
    public void testSilence() {
        String duration = "PT2H";
        String expression = "name='nerveCenterAlert' and severityCode<=4";
        SuppressEvent suppressEvent = new SuppressEvent();
        suppressEvent.setDuration(duration);
        suppressEvent.setExpression(expression);
        MetadataResource resource = new MetadataResource();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/testStream.json"), StreamDefinition.class);
        resource.addStreams(Collections.singletonList(streamDefinition));
        resource.addPolicy(MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/testPolicy.json"), PolicyDefinition.class));

        OpResult resp = resource.suppressPolicy("nerveCenter_port_down", "silence", suppressEvent);
        LOG.info("Response: {}/{}", resp.code, resp.message);

        PolicyDefinition policyDefinition = resource.getPolicyById("nerveCenter_port_down");
        Assert.assertNotNull(policyDefinition);
        Assert.assertNotNull(policyDefinition.getActiveSuppressEvent());
        Assert.assertEquals(SuppressType.silence, policyDefinition.getActiveSuppressEvent().getSuppressType());
        Assert.assertEquals(duration, policyDefinition.getActiveSuppressEvent().getDuration());
        Assert.assertEquals(expression, policyDefinition.getActiveSuppressEvent().getExpression());
        Assert.assertTrue(policyDefinition.getActiveSuppressEvent().getExpireTime().getTime() - System.currentTimeMillis() > 1.9 * 3600000);
    }

    @Test
    public void testPause() {
        String duration = "PT2H";
        String expression = "name='nerveCenterAlert' and severityCode<=4";
        SuppressEvent suppressEvent = new SuppressEvent();
        suppressEvent.setDuration(duration);
        suppressEvent.setExpression(expression);
        MetadataResource resource = new MetadataResource();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/testStream.json"), StreamDefinition.class);
        resource.addStreams(Collections.singletonList(streamDefinition));
        resource.addPolicy(MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/testPolicy.json"), PolicyDefinition.class));

        OpResult resp = resource.suppressPolicy("nerveCenter_port_down", "pause", suppressEvent);
        LOG.info("Response: {}/{}", resp.code, resp.message);

        PolicyDefinition policyDefinition = resource.getPolicyById("nerveCenter_port_down");
        Assert.assertNotNull(policyDefinition);
        Assert.assertNotNull(policyDefinition.getActiveSuppressEvent());
        Assert.assertEquals(SuppressType.pause, policyDefinition.getActiveSuppressEvent().getSuppressType());
        Assert.assertEquals(duration, policyDefinition.getActiveSuppressEvent().getDuration());
        Assert.assertEquals(expression, policyDefinition.getActiveSuppressEvent().getExpression());
        Assert.assertTrue(policyDefinition.getActiveSuppressEvent().getExpireTime().getTime() - System.currentTimeMillis() > 1.9 * 3600000);
    }

    @Test
    public void testExceptional() {
        String duration = "PT2H";
        String expression = "name='nerveCenterAlert' and severityCode<=4";
        SuppressEvent suppressEvent = new SuppressEvent();
        suppressEvent.setDuration(duration);
        suppressEvent.setExpression(expression);
        MetadataResource resource = new MetadataResource();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/testStream.json"), StreamDefinition.class);
        resource.addStreams(Collections.singletonList(streamDefinition));
        resource.addPolicy(MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/testPolicy.json"), PolicyDefinition.class));

        OpResult resp = resource.suppressPolicy("nerveCenter_port_down", "disable", suppressEvent);
        LOG.info("Response: {}/{}", resp.code, resp.message);

        PolicyDefinition policyDefinition = resource.getPolicyById("nerveCenter_port_down");
        Assert.assertNotNull(policyDefinition);
        Assert.assertNull(policyDefinition.getActiveSuppressEvent());
    }

    @Test
    public void testPause2() {
        String duration = "PT2H";
        String expression = "name='nerveCenterAlert' and severityCode<=4";
        SuppressEvent suppressEvent = new SuppressEvent();
        suppressEvent.setDuration(duration);
        suppressEvent.setExpireTime(new Date(System.currentTimeMillis() + 3 * 3600000));
        suppressEvent.setExpression(expression);
        MetadataResource resource = new MetadataResource();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/testStream.json"), StreamDefinition.class);
        resource.addStreams(Collections.singletonList(streamDefinition));
        resource.addPolicy(MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/testPolicy.json"), PolicyDefinition.class));

        OpResult resp = resource.suppressPolicy("nerveCenter_port_down", "pause", suppressEvent);
        LOG.info("Response: {}/{}", resp.code, resp.message);

        PolicyDefinition policyDefinition = resource.getPolicyById("nerveCenter_port_down");
        Assert.assertNotNull(policyDefinition);
        Assert.assertNotNull(policyDefinition.getActiveSuppressEvent());
        Assert.assertNotNull(policyDefinition.getActiveSuppressEvent().getStartTime());
        Assert.assertEquals(SuppressType.pause, policyDefinition.getActiveSuppressEvent().getSuppressType());
        Assert.assertEquals(duration, policyDefinition.getActiveSuppressEvent().getDuration());
        Assert.assertEquals(expression, policyDefinition.getActiveSuppressEvent().getExpression());
        Assert.assertTrue(policyDefinition.getActiveSuppressEvent().getExpireTime().getTime() - System.currentTimeMillis() > 2.9 * 3600000);
    }

    @Test
    public void testPause3() {
        String duration = "PT2H";
        String expression = "name='nerveCenterAlert' and severityCode<=4";
        SuppressEvent suppressEvent = new SuppressEvent();
        suppressEvent.setDuration(duration);
        suppressEvent.setExpireTime(new Date(System.currentTimeMillis() + 3 * 3600000));
        suppressEvent.setExpression(expression);
        MetadataResource resource = new MetadataResource();
        StreamDefinition streamDefinition = MetadataSerDeser
            .deserialize(getClass().getResourceAsStream("/testStream.json"), StreamDefinition.class);
        resource.addStreams(Collections.singletonList(streamDefinition));
        PolicyDefinition policyDefinition = MetadataSerDeser.deserialize(
            getClass().getResourceAsStream("/testPolicy2.json"), PolicyDefinition.class);
        policyDefinition.getActiveSuppressEvent().setExpireTime(new Date(System.currentTimeMillis() + 4 * 3600000));
        resource.addPolicy(policyDefinition);

        OpResult resp = resource.suppressPolicy("nerveCenter_port_down", "pause", suppressEvent);
        LOG.info("Response: {}/{}", resp.code, resp.message);

        policyDefinition = resource.getPolicyById("nerveCenter_port_down");
        Assert.assertNotNull(policyDefinition);
        Assert.assertNotNull(policyDefinition.getActiveSuppressEvent());
        Assert.assertTrue(policyDefinition.getActiveSuppressEvent().getExpireTime().getTime() - System.currentTimeMillis() > 2.9 * 3600000);
    }

}
