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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class LogExpressionTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(new LogExpression());
    }

    @Test
    public void testMatch() {
        testRunner.setProperty("Now: ", "${now()}");

        testRunner.enqueue("{data:{name: \"Esteban\", age: 32}}".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(LogExpression.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(LogExpression.REL_SUCCESS).get(0);
    }
    

}
