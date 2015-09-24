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
package com.cognitivemedicine.nifi.json.path;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.ObjectHolder;

@Tags({"json", "jsonPath"})
@CapabilityDescription("Evaluates one or more jsonPaths against the content of a FlowFile")
public class JsonPathProcessor extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the XPath evaluation are written to the FlowFile content or a FlowFile attribute; if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one XPath may be specified, and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder().name("matched").description("FlowFiles are routed to this relationship when the jsonPath is successfully evaluated and the FlowFile is modified as a result").build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder().name("unmatched").description("FlowFiles are routed to this relationship when the jsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles are routed to this relationship when the jsonPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid JSON, or if the XPath evaluates to multiple nodes").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DESTINATION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));
        return results;
    }
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .required(false)
                .addValidator(Validator.VALID)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final Map<String, String> attributeToJsonPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            attributeToJsonPathMap.put(entry.getKey().getName(), entry.getValue());
        }

        flowFileLoop:
        for (FlowFile flowFile : flowFiles) {

            final ObjectHolder<Throwable> error = new ObjectHolder<>(null);
            final ObjectHolder<Object> sourceRef = new ObjectHolder<>(null);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        Object document = Configuration.defaultConfiguration().jsonProvider().parse(rawIn, "UTF-8");
                        sourceRef.set(document);
                    } catch (final Exception e) {
                        error.set(e);
                    }
                }
            });

            if (error.get() != null) {
                logger.error("unable to evaluate jsonPath against {} due to {}; routing to 'failure'", new Object[]{flowFile, error.get()});
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> jsonPathResults = new HashMap<>();
            for (Map.Entry<String, String> attribute : attributeToJsonPathMap.entrySet()) {
                try {
                    String value = JsonPath.read(sourceRef.get(), attribute.getValue()).toString();
                    jsonPathResults.put(attribute.getKey(), value);
                } catch (PathNotFoundException e) {
                    //do nothing
                } catch (Exception e) {
                    logger.error("failed to evaluate jsonPath for {} for Property {} due to {}; routing to failure", new Object[]{flowFile, attribute.getKey(), e});
                    session.transfer(flowFile, REL_FAILURE);
                    continue flowFileLoop;
                }

            }

            if (error.get() == null) {
                flowFile = session.putAllAttributes(flowFile, jsonPathResults);
                final Relationship destRel = jsonPathResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                logger.info("Successfully evaluated jsonPath against {} and found {} matches; routing to {}", new Object[]{flowFile,
                    jsonPathResults.size(), destRel.getName()});
                session.transfer(flowFile, destRel);
                session.getProvenanceReporter().modifyAttributes(flowFile);
            } else {
                logger.error("Failed to write jsonPath result for {} due to {}; routing original to 'failure'", new Object[]{flowFile, error.get()});
                session.transfer(flowFile, REL_FAILURE);
            }
        }

    }
    
}
