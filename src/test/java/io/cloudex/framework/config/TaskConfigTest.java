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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.cloudex.framework.types.CodeLocation;
import io.cloudex.framework.types.ErrorAction;
import io.cloudex.framework.types.TargetType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TaskConfigTest {
    
    /**
     * Test method for {@link io.cloudex.framework.config.TaskConfig#valid()}.
     */
    @Test
    public void testValid() {
        TaskConfig config = setup(TargetType.COORDINATOR);
        assertTrue(config.valid());
        assertTrue(config.getValidationErrors().isEmpty());
        
        config = setup(TargetType.PROCESSOR);
        assertTrue(config.valid());
        assertTrue(config.getValidationErrors().isEmpty());
    }

    /**
     * Test method for {@link io.cloudex.framework.config.TaskConfig#getValidationErrors()}.
     */
    @Test
    public void testInvalidClassTaskName() {
        TaskConfig config = setup(TargetType.COORDINATOR);
        config.setTaskName("TaskName");
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("either taskName or className is required", config.getValidationErrors().get(0));
    }
    
    /**
     * Test method for {@link io.cloudex.framework.config.TaskConfig#getValidationErrors()}.
     */
    @Test
    public void testInvalidPartitionConfig() {
        TaskConfig config = setup(TargetType.PROCESSOR);
        config.getPartitioning().setType(null);
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("type may not be null", config.getValidationErrors().get(0));
        
        
        config = setup(TargetType.PROCESSOR);
        config.setPartitioning(null);
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("a valid partition config is required for processor tasks", config.getValidationErrors().get(0));
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testInvalidOutput() {
        TaskConfig config = setup(TargetType.PROCESSOR);
        config.setOutput((Set) Sets.newHashSet());
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("processor tasks should not have any output", config.getValidationErrors().get(0));
    }
    
    /**
     * 
     * @param target
     * @return
     */
    public static TaskConfig setup(TargetType target) {
        TaskConfig config = new TaskConfig();
        config.setClassName("test.blah.ClassName");
        CodeConfig code = new CodeConfig();
        code.setLocation(CodeLocation.LOCAL);
        config.setCode(code);
        config.setErrorAction(ErrorAction.CONTINUE);
        
        if(TargetType.PROCESSOR.equals(target)) {
            config.setPartitioning(PartitionConfigTest.setup());
            
        } else {
            Set<String> output = Sets.newHashSet("files", "items", "otherstuff");
            config.setOutput(output);
        }
        
        config.setTarget(target);
        
        Map<String, String> input = new HashMap<>();
        input.put("key", "#value");
        input.put("key1", "#value1");
        input.put("key2", "#value2");
        config.setInput(input);
        
        config.setId("task-config-1");
        
        return config;
    }

}
