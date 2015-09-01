/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2015, cloudex.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.cloudex.framework.cloud;

import static org.junit.Assert.*;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.types.CodeLocation;
import io.cloudex.framework.types.ProcessorStatus;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmMetaDataTest {
    
    private static String fingerprint =  "ddddvvvv";
    private static String cloudexException = "java.lang.Exception";
    private static String status = ProcessorStatus.BUSY.toString();
    private static String message = "Failed to run the task";
    private static String taskClass = "io.cloudex.tasks.MyTask";
    private static String codeLocation = CodeLocation.LOCAL.toString();
    private static String codeUrl = "http://cloudex.io/code.jar";
    
    private static String bucket = "bucket";
    private static String table = "table";
    private static String schemaFileName = "schemaFileName";
    
    private VmMetaData metaData;
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VmMetaData.CLOUDEX_EXCEPTION, cloudexException);
        attributes.put(VmMetaData.CLOUDEX_MESSAGE, message);
        attributes.put(VmMetaData.CLOUDEX_STATUS, status);
        attributes.put(VmMetaData.CLOUDEX_TASK_CLASS, taskClass);
        attributes.put(VmMetaData.CLOUDEX_TASK_CODE_TYPE, codeLocation);
        attributes.put(VmMetaData.CLOUDEX_TASK_CODE_URL, codeUrl);
        
        
        metaData = new VmMetaData(attributes, fingerprint);
        
        Assert.assertSame(attributes, metaData.getAttributes());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getStatus()}.
     */
    @Test
    public void testGetStatus() {
        assertEquals(status, metaData.getStatus());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getTaskCodeType()}.
     */
    @Test
    public void testGetTaskCodeType() {
        assertEquals(CodeLocation.LOCAL, metaData.getTaskCodeType());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getTaskClass()}.
     */
    @Test
    public void testGetTaskClass() {
        assertEquals(taskClass, metaData.getTaskClass());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getTaskCodeUrl()}.
     */
    @Test
    public void testGetTaskCodeUrl() {
        assertEquals(codeUrl, metaData.getTaskCodeUrl());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getProcessorStatus()}.
     */
    @Test
    public void testGetProcessorStatus() {
        assertEquals(ProcessorStatus.BUSY, metaData.getProcessorStatus());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getException()}.
     */
    @Test
    public void testGetException() {
        assertEquals(cloudexException, metaData.getException());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getMessage()}.
     */
    @Test
    public void testGetMessage() {
        assertEquals(message, metaData.getMessage());
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getUserMetaData()}.
     */
    @Test
    public void testGetUserMetaData() {
        metaData.addUserValue(schemaFileName, "schema_terms.json");
        metaData.addUserValue(bucket, "cloudex");
        metaData.addUserValue(table, "massive-table");
        
        Map<String, String> userData = metaData.getUserMetaData();
        assertNotNull(userData);
        assertEquals(3, userData.size());
        
        assertEquals("schema_terms.json", userData.get(schemaFileName));
        assertEquals("cloudex", userData.get(bucket));
        assertEquals("massive-table", userData.get(table));
        
        String schemaFileValue = metaData.getValue(VmMetaData.USER_PREFIX  + schemaFileName);
        assertNotNull(schemaFileValue);
        assertEquals("schema_terms.json", schemaFileValue);
        
        String bucketValue = metaData.getValue(VmMetaData.USER_PREFIX  + bucket);
        assertNotNull(bucketValue);
        assertEquals("cloudex", bucketValue);
        
        String tableValue = metaData.getValue(VmMetaData.USER_PREFIX  + table);
        assertNotNull(tableValue);
        assertEquals("massive-table", tableValue);
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmMetaData#getFollowUp(io.cloudex.framework.cloud.entities.VmMetaData)}.
     */
    @Test
    public void testGetFollowUp() {
        VmMetaData metaData1 = new VmMetaData();
        assertNull(metaData1.getFingerprint());
        
        VmMetaData metaData2 = this.metaData.getFollowUp(metaData1);
        assertSame(metaData1, metaData2);
        
        assertNotNull(metaData1.getFingerprint());
        assertEquals(fingerprint, metaData1.getFingerprint());

        metaData1 = this.metaData.getFollowUp(metaData1);
        assertNotNull(metaData1);
        assertNotNull(metaData1.getFingerprint());
        assertEquals(fingerprint, metaData1.getFingerprint());
        
        
        
    }

}
