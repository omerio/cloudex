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

import static org.junit.Assert.*;
import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.types.PartitionType;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionConfigTest {
    
    private PartitionConfig config;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        config = setup();
        
       
    }

    /**
     * Test method for {@link io.cloudex.framework.config.PartitionConfig#valid()}.
     */
    @Test
    public void testValid() {

        assertTrue(config.valid());
        assertTrue(config.getValidationErrors().isEmpty());
        
        config.setFunctionName("BlahFunction");
        config.setClassName(null);
        assertTrue(config.valid());
        assertTrue(config.getValidationErrors().isEmpty());
        
        config.setType(PartitionType.ITEMS);
        config.setOutput(null);
        assertTrue(config.valid());
        assertTrue(config.getValidationErrors().isEmpty());
    }

    
    @Test
    public void testInValidClassFunctionNames() {
        config.setFunctionName("TestFunction");
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("either functionName or className is required", config.getValidationErrors().get(0));
        
    }
    /**
     * Test method for {@link io.cloudex.framework.config.PartitionConfig#getValidationErrors()}.
     */
    @Test
    public void testInValidItems() {
                
        config.getInput().remove(PartitionFunction.ITEMS_KEY);
        config.getInput().put("blah", "#blahvalue");
        
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("expecting an input with key items", config.getValidationErrors().get(0));
    }
    
    @Test
    public void testValidTypeItems() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.ITEMS);
       
        Map<String, String> input = new HashMap<>();
        input.put(PartitionFunction.ITEMS_KEY, "#items");
        config.setInput(input);
        
        assertTrue(config.valid());
        assertEquals(0, config.getValidationErrors().size());
        
    }
    
    @Test
    public void testValidTypeCountRef() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.COUNT);
        config.setCountRef("#noOfProcessors");
        
        assertTrue(config.valid());
        assertEquals(0, config.getValidationErrors().size());
    }
    
    @Test
    public void testValidTypeCount() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.COUNT);
        config.setCount(5);
        
        assertTrue(config.valid());
        assertEquals(0, config.getValidationErrors().size());
    }
    
    @Test
    public void testInValidTypeCount() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.COUNT);
        //config.setCount(5);
        
        assertFalse(config.valid());
        System.out.println(config.getValidationErrors());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("either count or countRef is required for Function type count", 
                config.getValidationErrors().get(0));
    }
    
    @Test
    public void testInValidOutput() {
        
        config.setOutput(null);
        
        assertFalse(config.valid());
        assertEquals(1, config.getValidationErrors().size());
        assertEquals("output is required for Function type partition", config.getValidationErrors().get(0));
    }
    
    public static PartitionConfig setup() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.FUNCTION);
        
        config.setOutput("test");
        config.setClassName("test.com.ClassName");
        
        Map<String, String> input = new HashMap<>();
        input.put(PartitionFunction.ITEMS_KEY, "#items");
        config.setInput(input);
        return config;
    }

}
