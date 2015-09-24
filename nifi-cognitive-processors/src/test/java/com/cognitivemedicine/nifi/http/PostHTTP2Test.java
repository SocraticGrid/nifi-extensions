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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import org.mockserver.mock.Expectation;
import org.mockserver.model.StringBody;

public class PostHTTP2Test {

    private TestRunner testRunner;
    private ClientAndServer mockServer;

    private final String predefinedPOSTResponse = "{\"success\": true, \"message\": \"Alles Gut!\"}";
    
    @Before
    public void init() {
        Map<String, String> postResources = new HashMap<>();
        postResources.put("/do-post", predefinedPOSTResponse);

        this.mockServer = this.startMockServer(8585, null, postResources, null);
        
        testRunner = TestRunners.newTestRunner(new PostHTTP2());
    }
    
    @After
    public void doAfter(){
        this.mockServer.stop();
    }

    @Test
    public void testVariableURL() {
        testRunner.setProperty(PostAdvancedHTTP.URL, "http://${test.host}:${test.port}/${test.context}");
        
        String flowFileContent = "{data:{name: \"Esteban\", age: 32}}";
        
        Map<String,String> attributes = new HashMap<>();
        attributes.put("test.host", "localhost");
        attributes.put("test.port", "8585");
        attributes.put("test.context", "do-post");
        testRunner.enqueue(flowFileContent.getBytes(), attributes);
        testRunner.assertValid();
        testRunner.run();
        
        testRunner.assertAllFlowFilesTransferred(PostHTTP2.REL_SUCCESS);
        
        
        Expectation[] invocations = this.mockServer.retrieveAsExpectations(HttpRequest.request()
                .withMethod("POST")
                .withPath("/do-post"));
        assertThat(Arrays.asList(invocations), hasSize(1));
        
        StringBody body = (StringBody)invocations[0].getHttpRequest().getBody();

        assertThat(body.getValue(), is(flowFileContent));
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
