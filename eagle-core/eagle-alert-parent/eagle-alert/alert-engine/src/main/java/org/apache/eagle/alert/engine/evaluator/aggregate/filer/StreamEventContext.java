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

import org.apache.commons.jexl3.JexlContext;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.StreamEvent;

public class StreamEventContext implements IStreamFilterContext, JexlContext {

    private final StreamEvent event;
    private final StreamDefinition definition;

    public StreamEventContext(StreamEvent event, StreamDefinition def) {
        this.event = event;
        this.definition = def;
    }

    @Override
    public Object get(String arg0) {
        if (definition.getColumnIndex(arg0) < 0) {
            // this means the arg0 is not defined in the definition
            // keep it as empty string here to let JEXL can use empty(arg0) here without exception
            return null;
        }
        return event.getData(definition, arg0)[0];
    }

    @Override
    public boolean has(String arg0) {
        return true;
    }

    @Override
    public void set(String arg0, Object arg1) {
    }

}
