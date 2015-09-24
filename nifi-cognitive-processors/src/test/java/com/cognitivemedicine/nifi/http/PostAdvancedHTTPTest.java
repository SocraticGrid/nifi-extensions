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
package com.cognitivemedicine.nifi.http;

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

public class PostAdvancedHTTPTest {

    private TestRunner testRunner;
    private ClientAndServer mockServer;

    private final String predefinedPOSTResponse = "{\"success\": true, \"message\": \"Alles Gut!\"}";
    
    @Before
    public void init() {
        Map<String, String> postResources = new HashMap<>();
        postResources.put("/do-post", predefinedPOSTResponse);

        this.mockServer = this.startMockServer(8585, null, postResources, null);
        
        testRunner = TestRunners.newTestRunner(new PostAdvancedHTTP());
    }
    
    @After
    public void doAfter(){
        this.mockServer.stop();
    }

    @Test
    public void testInvalidAttribute() {
        testRunner.setProperty(PostAdvancedHTTP.URL, "http://localhost:8585/do-post");
        testRunner.setProperty(PostAdvancedHTTP.DESTINATION, PostAdvancedHTTP.DESTINATION_ATTRIBUTE);
        
        //PostAdvancedHTTP.DESTINATION_NAME is not set
        //testRunner.setProperty(PostAdvancedHTTP.DESTINATION_NAME, "response-content");

        testRunner.enqueue("{data:{name: \"Esteban\", age: 32}}".getBytes());
        try {
            testRunner.assertValid();
            Assert.fail("Exception expected!");
        } catch (AssertionError e){
            //expected
            Assert.assertTrue(e.getMessage().contains("'Destination' is invalid because Destination is set to flowfile-attribute but Destination Name is not set"));
        }

    }

    @Test
    public void testAttribute() {
        testRunner.setProperty(PostAdvancedHTTP.URL, "http://localhost:8585/do-post");
        testRunner.setProperty(PostAdvancedHTTP.DESTINATION, PostAdvancedHTTP.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(PostAdvancedHTTP.DESTINATION_NAME, "response-content");

        testRunner.enqueue("{data:{name: \"Esteban\", age: 32}}".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PostAdvancedHTTP.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(PostAdvancedHTTP.REL_SUCCESS).get(0);
        out.assertAttributeEquals("response-content", predefinedPOSTResponse);
        out.assertContentEquals("{data:{name: \"Esteban\", age: 32}}");
    }
    
    @Test
    public void testIgnore() {
        testRunner.setProperty(PostAdvancedHTTP.URL, "http://localhost:8585/do-post");
        testRunner.setProperty(PostAdvancedHTTP.DESTINATION, PostAdvancedHTTP.DESTINATION_IGNORE);

        testRunner.enqueue("{data:{name: \"Esteban\", age: 32}}".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PostAdvancedHTTP.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(PostAdvancedHTTP.REL_SUCCESS).get(0);
        out.assertContentEquals("{data:{name: \"Esteban\", age: 32}}");
    }
    
    @Test
    public void testContent() {
        testRunner.setProperty(PostAdvancedHTTP.URL, "http://localhost:8585/do-post");
        testRunner.setProperty(PostAdvancedHTTP.DESTINATION, PostAdvancedHTTP.DESTINATION_CONTENT);

        testRunner.enqueue("{data:{name: \"Esteban\", age: 32}}".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PostAdvancedHTTP.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(PostAdvancedHTTP.REL_SUCCESS).get(0);
        out.assertContentEquals(predefinedPOSTResponse);
    }

    protected ClientAndServer startMockServer(int port, Map<String, String> getResources, Map<String, String> postResources, Map<String, String> putResources) {
        ClientAndServer mockServer = startClientAndServer(port);

        if (getResources != null) {
            for (Map.Entry<String, String> entry : getResources.entrySet()) {
                this.configureServerResource(mockServer, entry.getKey(), "GET", entry.getValue());
            }
        }

        if (postResources != null) {
            for (Map.Entry<String, String> entry : postResources.entrySet()) {
                this.configureServerResource(mockServer, entry.getKey(), "POST", entry.getValue());
            }
        }

        if (putResources != null) {
            for (Map.Entry<String, String> entry : putResources.entrySet()) {
                this.configureServerResource(mockServer, entry.getKey(), "PUT", entry.getValue());
            }
        }

        return mockServer;
    }

    private void configureServerResource(ClientAndServer server, String url, String operation, String body) {
        server.when(
                HttpRequest.request()
                .withMethod(operation.toUpperCase())
                .withPath(url)
        )
                .respond(
                        HttpResponse.response()
                        .withStatusCode(200)
                        .withHeader(new Header("Content-Type", "application/json; charset=utf-8"))
                        .withBody(body));
    }

}
