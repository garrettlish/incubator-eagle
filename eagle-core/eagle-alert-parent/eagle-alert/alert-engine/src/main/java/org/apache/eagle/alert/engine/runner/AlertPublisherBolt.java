/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.evaluator.aggregate.filer.StreamEventContext;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishSpecListener;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.AlertStreamFilter;
import org.apache.eagle.alert.engine.publisher.PipeStreamFilter;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.publisher.template.AlertTemplateEngine;
import org.apache.eagle.alert.engine.publisher.template.AlertTemplateProvider;
import org.apache.eagle.alert.utils.AlertConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AlertPublisherBolt extends AbstractStreamBolt implements AlertPublishSpecListener {

    private static final Logger LOG = LoggerFactory.getLogger(AlertPublisherBolt.class);

    private final AlertPublisher alertPublisher;
    private volatile Map<String, Publishment> cachedPublishments = new HashMap<>();
    private volatile Map<String, PolicyDefinition> policyDefinitionMap;
    private volatile Map<String, StreamDefinition> streamDefinitionMap;
    private volatile Map<String, JexlExpression> jexlExpressionMap;
    private AlertTemplateEngine alertTemplateEngine;

    private boolean logEventEnabled;
    private TopologyContext context;
    private AlertStreamFilter alertFilter;

    private JexlEngine engine;

    public AlertPublisherBolt(String alertPublisherName, Config config, IMetadataChangeNotifyService coordinatorService) {
        super(alertPublisherName, coordinatorService, config);
        this.alertPublisher = new AlertPublisherImpl(alertPublisherName);

        if (config != null && config.hasPath("topology.logEventEnabled")) {
            logEventEnabled = config.getBoolean("topology.logEventEnabled");
        }
    }

    @Override
    public void internalPrepare(OutputCollector collector, IMetadataChangeNotifyService coordinatorService, Config config, TopologyContext context) {
        coordinatorService.registerListener(this);
        coordinatorService.init(config, MetadataType.ALERT_PUBLISH_BOLT);
        this.alertPublisher.init(config, stormConf);
        streamContext = new StreamContextImpl(config, context.registerMetric("eagle.publisher", new MultiCountMetric(), 60), context);
        this.context = context;
        this.alertTemplateEngine = AlertTemplateProvider.createAlertTemplateEngine();
        this.alertTemplateEngine.init(config);
        this.alertFilter = new PipeStreamFilter(new AlertContextEnrichFilter(this), new AlertTemplateFilter(alertTemplateEngine));

        this.engine = new JexlBuilder().create();
        this.jexlExpressionMap = Maps.newConcurrentMap();
    }

    @Override
    public void execute(Tuple input) {
        try {
            streamContext.counter().incr("receive_count");
            PublishPartition partition = (PublishPartition) input.getValueByField(AlertConstants.FIELD_0);
            AlertStreamEvent event = (AlertStreamEvent) input.getValueByField(AlertConstants.FIELD_1);
            if (logEventEnabled) {
                LOG.info("Alert publish bolt {}/{} with partition {} received event: {}",
                    this.getBoltId(), this.context.getThisTaskId(), partition, event);
            }
            AlertStreamEvent filteredEvent = alertFilter.filter(event);

            String policyId = partition.getPolicyId();
            JexlExpression jexlExpression = this.jexlExpressionMap.get(policyId);
            filteredEvent = new AlertSuppressEnrichFilter(policyId, jexlExpression).filter(filteredEvent);

            if (filteredEvent != null) {
                if (filteredEvent.getContext().containsKey(SuppressType.pause.name())
                    && (Boolean) filteredEvent.getContext().get(SuppressType.pause.name())) {
                    LOG.trace("Alert {} pause=true, skipped publish", filteredEvent);
                } else {
                    alertPublisher.nextEvent(partition, filteredEvent);
                }
            }
            this.collector.ack(input);
            streamContext.counter().incr("ack_count");
        } catch (Throwable ex) {
            streamContext.counter().incr("fail_count");
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        }
    }

    @Override
    public void cleanup() {
        alertPublisher.close();
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    @Override
    public synchronized void onAlertPublishSpecChange(PublishSpec pubSpec, Map<String, StreamDefinition> sds) {
        if (pubSpec == null) {
            return;
        }
        this.streamDefinitionMap = sds;

        List<Publishment> newPublishments = pubSpec.getPublishments();
        if (newPublishments == null) {
            LOG.info("no publishments with PublishSpec {} for this topology", pubSpec);
            return;
        }

        Map<String, Publishment> newPublishmentsMap = new HashMap<>();
        newPublishments.forEach(p -> newPublishmentsMap.put(p.getName(), p));
        MapComparator<String, Publishment> comparator = new MapComparator<>(newPublishmentsMap, cachedPublishments);
        comparator.compare();

        List<Publishment> beforeModified = new ArrayList<>();
        comparator.getModified().forEach(p -> beforeModified.add(cachedPublishments.get(p.getName())));
        alertPublisher.onPublishChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), beforeModified);

        // switch
        cachedPublishments = newPublishmentsMap;
        specVersion = pubSpec.getVersion();
    }

    @Override
    public synchronized void onAlertPolicyChange(Map<String, PolicyDefinition> pds, Map<String, StreamDefinition> sds) {
        List<String> policyToRemove = new ArrayList<>();
        if (this.policyDefinitionMap != null) {
            policyToRemove.addAll(this.policyDefinitionMap.keySet().stream().filter(policyId -> !pds.containsKey(policyId)).collect(Collectors.toList()));
        }

        this.policyDefinitionMap = pds;
        this.streamDefinitionMap = sds;

        for (Map.Entry<String, PolicyDefinition> entry : pds.entrySet()) {
            try {
                this.alertTemplateEngine.register(entry.getValue());

                // update jexl expression map
                String policyName = entry.getValue().getName();
                SuppressEvent suppressEvent = entry.getValue().getActiveSuppressEvent();
                if (this.jexlExpressionMap.containsKey(policyName)) {
                    JexlExpression jexlExpression = this.jexlExpressionMap.get(policyName);
                    if (suppressEvent == null || StringUtils.isNotBlank(suppressEvent.getExpression())) {
                        this.jexlExpressionMap.remove(policyName);
                    } else if (!suppressEvent.getExpression().equals(jexlExpression.getSourceText())) {
                        this.jexlExpressionMap.remove(policyName);
                    }
                }
                if (suppressEvent != null && StringUtils.isNotBlank(suppressEvent.getExpression())) {
                    this.jexlExpressionMap.put(policyName, engine.createExpression(suppressEvent.getExpression()));
                }
            } catch (Throwable throwable) {
                LOG.error("Failed to register policy {} in template engine", entry.getKey(), throwable);
            }
        }

        for (String policyId : policyToRemove) {
            try {
                this.alertTemplateEngine.unregister(policyId);
                this.jexlExpressionMap.remove(policyId);
            } catch (Throwable throwable) {
                LOG.error("Failed to unregister policy {} from template engine", policyId, throwable);
            }
        }
    }

    private class AlertContextEnrichFilter implements AlertStreamFilter {
        private final AlertPublisherBolt alertPublisherBolt;

        private AlertContextEnrichFilter(AlertPublisherBolt alertPublisherBolt) {
            this.alertPublisherBolt = alertPublisherBolt;
        }

        /**
         * TODO: Refactor wrapAlertPublishEvent into alertTemplateEngine and remove extraData from AlertStreamEvent.
         */
        @Override
        public AlertStreamEvent filter(AlertStreamEvent event) {
            event.ensureAlertId();
            Map<String, Object> extraData = new HashMap<>();
            List<String> appIds = new ArrayList<>();
            if (alertPublisherBolt.policyDefinitionMap == null || alertPublisherBolt.streamDefinitionMap == null) {
                LOG.warn("policyDefinitions or streamDefinitions in publisher bolt have not been initialized");
            } else {
                PolicyDefinition policyDefinition = alertPublisherBolt.policyDefinitionMap.get(event.getPolicyId());
                if (alertPublisherBolt.policyDefinitionMap != null && policyDefinition != null) {
                    for (String inputStreamId : policyDefinition.getInputStreams()) {
                        StreamDefinition sd = alertPublisherBolt.streamDefinitionMap.get(inputStreamId);
                        if (sd != null) {
                            extraData.put(AlertPublishEvent.SITE_ID_KEY, sd.getSiteId());
                            appIds.add(sd.getStreamSource());
                        }
                    }
                    extraData.put(AlertPublishEvent.APP_IDS_KEY, appIds);
                    extraData.put(AlertPublishEvent.POLICY_VALUE_KEY, policyDefinition.getDefinition().getValue());
                    event.setSeverity(policyDefinition.getAlertSeverity());
                    event.setCategory(policyDefinition.getAlertCategory());
                }
                event.setContext(extraData);
            }
            return event;
        }
    }

    private class AlertTemplateFilter implements AlertStreamFilter {
        private final AlertTemplateEngine alertTemplateEngine;

        private AlertTemplateFilter(AlertTemplateEngine alertTemplateEngine) {
            this.alertTemplateEngine = alertTemplateEngine;
        }

        @Override
        public AlertStreamEvent filter(AlertStreamEvent event) {
            return this.alertTemplateEngine.filter(event);
        }
    }

    private class AlertSuppressEnrichFilter implements AlertStreamFilter {

        private String policyId;
        private JexlExpression jexlExpression;

        public AlertSuppressEnrichFilter(String policyId, JexlExpression jexlExpression) {
            this.policyId = policyId;
            this.jexlExpression = jexlExpression;
        }

        @Override
        public AlertStreamEvent filter(AlertStreamEvent filteredEvent) {
            if (filteredEvent == null) {
                return null;
            }
            PolicyDefinition policyDefinition = Optional.ofNullable(policyDefinitionMap)
                .orElse(Maps.newHashMap())
                .get(policyId);
            SuppressEvent suppressEvent = Optional.ofNullable(policyDefinition)
                .orElse(new PolicyDefinition())
                .getActiveSuppressEvent();
            // mark the event silence=true if suppressEvent is not expired and filter expression matches
            Optional.ofNullable(suppressEvent)
                .filter(se -> se != null
                    && se.getExpireTime() != null
                    && se.getExpireTime().getTime() > System.currentTimeMillis())
                .ifPresent(se -> {
                    if (jexlExpression == null) {
                        filteredEvent.getContext().put(se.getSuppressType().name(), true);
                    } else {
                        Optional.ofNullable(policyDefinition)
                            .filter(pd -> pd != null && pd.getOutputStreams().size() > 0)
                            .ifPresent(pd -> {
                                String exp = null;
                                try {
                                    exp = jexlExpression.getSourceText();
                                    // TODO more than one output stream will choose the first one
                                    Object silenced = jexlExpression.evaluate(
                                        new StreamEventContext(filteredEvent,
                                            streamDefinitionMap.get(pd.getOutputStreams().get(0))));
                                    Optional.ofNullable(silenced)
                                        .filter(s -> s instanceof Boolean && (Boolean) s)
                                        .ifPresent(s -> filteredEvent.getContext().put(se.getSuppressType().name(), true));
                                } catch (Exception e) {
                                    LOG.warn("Failed to evaluate event for expression: " + exp, e);
                                }
                            });
                    }
                });
            return filteredEvent;
        }
    }

}