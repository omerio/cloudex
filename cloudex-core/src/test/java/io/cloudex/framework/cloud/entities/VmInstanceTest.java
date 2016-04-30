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


package io.cloudex.framework.cloud.entities;

import static org.junit.Assert.*;
import io.cloudex.framework.config.VmConfig;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmInstanceTest {
    
    private static final double COST = 0.252;
    
    private static final long MIN_USE = 600l;
    
    private VmInstance instance;
    

    @Before
    public void setUp() throws Exception {
        VmConfig config = new VmConfig();
        config.setCores(4);
        config.setCost(COST);
        config.setMemory(26);
        config.setMinUsage(MIN_USE);
        config.setReuse(true);
        config.setVmType("n1-highmem-4");
        
        instance = new VmInstance(config, new Date());
    }
    
    @Test
    public void testGetCostNoCost() {
        instance.getVmConfig().setCost(null);
        instance.getVmConfig().setMinUsage(null);
        double cost = instance.getCost();
        assertEquals(0.0, cost, 0);
       
    }

    /**
     * Test method for {@link io.cloudex.framework.cloud.entities.VmInstance#getCost()}.
     */
    @Test
    public void testGetCostMinUsage() {
        double cost = instance.getCost();
        assertTrue(cost != 0);
        assertEquals(COST * MIN_USE / 3600, cost, 0);
       
    }
    
    @Test
    public void testGetCostHourUsage() {

        instance.setEnd(DateUtils.addHours(instance.getStart(), 1));
        
        double cost = instance.getCost();
        assertEquals(COST, cost, 0);
    }
    
    @Test
    public void testGetCostNearMinUsage() {
        // 12.5 minutes
        Date end = DateUtils.addSeconds(instance.getStart(), (60 * 12) + 30);

        instance.setEnd(end);
        
        double cost = instance.getCost();
        
        double expected = (COST / 3600) * 13 * 60; 
        assertEquals(expected, cost, 0.00001);
    }

}
