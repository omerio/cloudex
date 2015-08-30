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


package io.cloudex.framework.components;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.cloudex.framework.cloud.CloudService;
import io.cloudex.framework.cloud.VmMetaData;
import io.cloudex.framework.config.Job;
import io.cloudex.framework.config.JobTest;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.types.ErrorAction;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
@RunWith(JMockit.class)
public class CoordinatorTest {
    
    public static final String SCHEMA_TERMS_FILE_VALUE = "schema_terms_file.json";
    public static final String SCHEMA_TERMS_FILE_KEY = "schemaTermsFile";
    
    public static final String BUCKET_KEY = "bucket";
    public static final String BUCKET_VALUE = "testBucket";
    
    public static final String SCHEMA_KEY = "schema";
    public static final String SCHEMA_VALUE = "myschemafile.txt";
    
    private VmMetaData metaData;
    
    private CloudService cloudService;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        
        metaData = new VmMetaData();
        
        cloudService = new MockUp<CloudService>() {
            @Mock(invocations = 1)
            public VmMetaData init() throws IOException {
                return metaData;
            }
        }.getMockInstance();
        
        assertNotNull(cloudService);
        
    }

    /**
     * Test method for {@link io.cloudex.framework.components.Coordinator#run()}.
     * @throws IOException 
     */
    @Test
    public void testCreateSuccess() throws IOException {
        Job job = JobTest.loadJob("CoordinatorTest.json");
        assertNotNull(job);
        assertNotNull(job.getTasks());
        assertEquals(3, job.getTasks().size());
        assertTrue(job.valid());
        
        Coordinator coordinator = new Coordinator(job, cloudService);
        
        assertNotNull(coordinator);
               
        Context context = coordinator.getContext();
        assertNotNull(context);
        Map<String, Object> data = job.getData();
        
        // check context contains all of the job data
        for(Entry<String, Object> entry: data.entrySet()) {
            assertTrue(context.containsKey(entry.getKey()));   
            assertEquals(entry.getValue(), context.get(entry.getKey()));
        }
        
        // we have empty processors
        assertTrue(coordinator.getProcessors().isEmpty());
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateFailJob() throws IOException {
        Job job = JobTest.loadJob("CoordinatorTest.json");
        job.setVmConfig(null);
        new Coordinator(job, cloudService);
    }
    
    @Test
    public void testRunCoordinatorTask() throws IOException {
        Job job = JobTest.loadJob("CoordinatorTest.json");
        TaskConfig task = job.getTasks().get(0);
        job.setTasks(Lists.newArrayList(task));
        
        
        Coordinator coordinator = new Coordinator(job, cloudService);
        Context context = coordinator.getContext();
        assertFalse(context.containsKey(SCHEMA_TERMS_FILE_KEY));
        
        coordinator.run();
        
        assertTrue(context.containsKey(SCHEMA_TERMS_FILE_KEY));
        assertEquals(SCHEMA_TERMS_FILE_VALUE, context.get(SCHEMA_TERMS_FILE_KEY));
        
        assertFalse(context.containsKey("somekey"));
        assertTrue(coordinator.getProcessors().isEmpty());
    }
    
    @Test(expected = IOException.class)
    public void testRunTaskInputFail() throws IOException {
        Job job = JobTest.loadJob("CoordinatorTest.json");
        TaskConfig task = job.getTasks().get(0);
        job.setTasks(Lists.newArrayList(task));
        task.getInput().put("age", "50");
        
        Coordinator coordinator = new Coordinator(job, cloudService);
        coordinator.run();
    }
    
    
    @Test
    public void testRunTaskInputContinue() throws IOException {
        Job job = JobTest.loadJob("CoordinatorTest.json");
        TaskConfig task = job.getTasks().get(0);
        job.setTasks(Lists.newArrayList(task));
        task.setErrorAction(ErrorAction.CONTINUE);
        job.getData().put(BUCKET_KEY, "Error");
        
        Coordinator coordinator = new Coordinator(job, cloudService);
        coordinator.run();
        
        Context context = coordinator.getContext();
        assertFalse(context.containsKey(SCHEMA_TERMS_FILE_KEY));
    }

}
