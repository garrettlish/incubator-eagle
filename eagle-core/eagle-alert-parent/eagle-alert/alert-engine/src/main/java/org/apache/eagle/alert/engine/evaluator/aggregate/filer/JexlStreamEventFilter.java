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
package org.apache.eagle.alert.engine.evaluator.aggregate.filer;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JexlStreamEventFilter implements IStreamEventFilter<StreamEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(JexlStreamEventFilter.class);

    private final JexlEngine engine = new JexlBuilder().create();
    private JexlExpression exp;

    public JexlStreamEventFilter(String expression) {
        exp = engine.createExpression(expression);
    }

    @Override
    public boolean evaluate(IStreamFilterContext context) {
        try {
            Object obj = exp.evaluate((JexlContext) context);
            if (obj instanceof Boolean) {
                return (Boolean) obj;
            }
            LOG.warn("stream filter return non-boolean result, treat as false! Expression: {}, result {}",
                    exp.getSourceText(), obj);
        } catch (Exception e) {
            LOG.error("evaluation failed, expression need to be refined!", exp);
        }
        return false;
    }

    @Override
    public void close() {
    }

}
