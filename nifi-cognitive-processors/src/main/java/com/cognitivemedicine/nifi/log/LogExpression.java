/* 
 * Copyright 2015 Cognitive Medical Systems, Inc (http://www.cognitivemedciine.com).
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
package com.cognitivemedicine.nifi.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"expression", "logging"})
@CapabilityDescription("Logs expressions. Each dynamic property will be logged.")
public class LogExpression extends AbstractProcessor {

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .required(true)
            .description("The Log Level to use when logging.")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();

    public static final String FIFTY_DASHES = "--------------------------------------------------";

    public static enum DebugLevels {

        trace, debug, info, warn, error
    }

    public static final long ONE_MB = 1024 * 1024;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> supportedDescriptors;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to this relationship").build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(LOG_LEVEL);
        supportedDescriptors = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    protected String processFlowFile(final ProcessorLog logger, final DebugLevels logLevel, final FlowFile flowFile, final ProcessSession session, final ProcessContext context) {

        final ProcessorLog LOG = getLogger();

        // Pretty print metadata
        final StringBuilder message = new StringBuilder();
        message.append("\n");
        message.append(FIFTY_DASHES);
        message.append("\nExpressions");
        for (Map.Entry<PropertyDescriptor, String> prop : context.getProperties().entrySet()) {
            if (prop.getKey().isDynamic()){
                String value = context.getProperty(prop.getKey()).evaluateAttributeExpressions(flowFile).getValue();
                message.append(String.format("\n%1$s:\t'%2$s'", prop.getKey().getName(), value));
            }
        }
        message.append("\n");
        message.append(FIFTY_DASHES);

        final String outputMessage = message.toString().trim();
        // Uses optional property to specify logging level
        switch (logLevel) {
            case info:
                LOG.info(outputMessage);
                break;
            case debug:
                LOG.debug(outputMessage);
                break;
            case warn:
                LOG.warn(outputMessage);
                break;
            case trace:
                LOG.trace(outputMessage);
                break;
            case error:
                LOG.error(outputMessage);
                break;
            default:
                LOG.debug(outputMessage);
        }

        return outputMessage;

    }

    private void transferChunk(final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (!flowFiles.isEmpty()) {
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String logLevelValue = context.getProperty(LOG_LEVEL).getValue().toLowerCase();

        final DebugLevels logLevel;
        try {
            logLevel = DebugLevels.valueOf(logLevelValue);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        final ProcessorLog LOG = getLogger();
        boolean isLogLevelEnabled = false;
        switch (logLevel) {
            case trace:
                isLogLevelEnabled = LOG.isTraceEnabled();
                break;
            case debug:
                isLogLevelEnabled = LOG.isDebugEnabled();
                break;
            case info:
                isLogLevelEnabled = LOG.isInfoEnabled();
                break;
            case warn:
                isLogLevelEnabled = LOG.isWarnEnabled();
                break;
            case error:
                isLogLevelEnabled = LOG.isErrorEnabled();
                break;
        }

        if (!isLogLevelEnabled) {
            transferChunk(session);
            return;
        }

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        processFlowFile(LOG, logLevel, flowFile, session, context);
        session.transfer(flowFile, REL_SUCCESS);
    }

}
