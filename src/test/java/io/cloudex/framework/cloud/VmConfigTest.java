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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmConfigTest {
    
    private VmConfig config;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        config = setup();
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.VmConfig#valid()}.
     */
    @Test
    public void testValid() {
        assertTrue(config.valid());
        
        config.setDiskType("d");
        assertTrue(config.valid());
    }
    
    @Test
    public void testNotValid() {
        config.setDiskType(null);
        assertFalse(config.valid());
        
        List<String> messages = config.getValidationErrors();
        System.out.println(messages);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        assertEquals("diskType may not be null", messages.get(0));
        
        config.setZoneId(null);
        assertFalse(config.valid());
        assertEquals(2, config.getValidationErrors().size());
        
        
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.VmConfig#getValidationErrors()}.
     */
    @Test
    public void testGetValidationErrors() {
        assertTrue(config.getValidationErrors().isEmpty());
    }
    
    public static VmConfig setup() {
        VmConfig config = new VmConfig();
        config.setDiskType("disktype");
        config.setImageId("imageid");
        config.setNetworkId("networkid");
        config.setVmType("vmtype");
        config.setZoneId("zoneid");
        return config;
    }

}
