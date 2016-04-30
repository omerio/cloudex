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


package io.cloudex.framework.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.cloudex.framework.cloud.VmConfigTest;
import io.cloudex.framework.types.ExecutionMode;
import io.cloudex.framework.types.TargetType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class JobTest {

    
    /**
     * Test method for {@link io.cloudex.framework.config.Job#fromJsonFile(java.lang.String)}.
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    public void testFromJsonFile() throws FileNotFoundException, IOException {
        Job job = loadJob("JobTest.json");
        System.out.println(job.getValidationErrors());
        assertTrue(job.valid());
        assertTrue(job.getValidationErrors().isEmpty());
    }

    /**
     * Test method for {@link io.cloudex.framework.config.Job#valid()}.
     */
    @Test
    public void testValid() {
        Job job = setup();
        assertTrue(job.valid());
        assertTrue(job.getValidationErrors().isEmpty());
    }

    /**
     * Test method for {@link io.cloudex.framework.config.Job#getValidationErrors()}.
     */
    @Test
    public void testInvalid() {
        Job job = setup();
        
        job.setData(new HashMap<String, Object>());
        job.setTasks(new ArrayList<TaskConfig>());
        
        assertFalse(job.valid());
        assertEquals(2, job.getValidationErrors().size());
        
        job = setup();
        job.getTasks().get(0).setErrorAction(null);
        job.getTasks().get(1).setTarget(null);
        
        assertFalse(job.valid());
        assertEquals(2, job.getValidationErrors().size());
    }
   
    /**
     * 
     * @return
     */
    public static Job setup() {
        
        Map<String, Object> data = new HashMap<>();
        data.put("bucket", "mybucket");
        data.put("schema", "testschema");
        data.put("threshold", 3.56);
        data.put("nodes", 5);
        
        Job job = new Job();
        job.setData(data);
        
        job.setId("my-job-id");
        job.setMode(ExecutionMode.SERIAL);
        job.setVmConfig(VmConfigTest.setup());
        
        List<TaskConfig> tasks = new ArrayList<>();
        tasks.add(TaskConfigTest.setup(TargetType.COORDINATOR));
        tasks.add(TaskConfigTest.setup(TargetType.PROCESSOR));
        job.setTasks(tasks);
        
        return job;
    }
    
    public static Job loadJob(String filename) throws FileNotFoundException, IOException {
        URL url = JobTest.class.getClassLoader().getResource(filename);
        assertNotNull(url);
        Job job = Job.fromJsonFile(url.getPath());
        return job;
    }

}
