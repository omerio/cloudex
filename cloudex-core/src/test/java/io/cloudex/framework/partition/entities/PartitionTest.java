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


package io.cloudex.framework.partition.entities;

import static org.junit.Assert.assertEquals;
import io.cloudex.framework.partition.builtin.BinPackingPartition;
import io.cloudex.framework.partition.builtin.BinPackingPartitionTest;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionTest {
    
    private List<Partition> bins;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        BinPackingPartition function = new BinPackingPartition(BinPackingPartitionTest.createItems()); 
        function.setMaxBinItems(10L);
        function.setNewBinPercentage(0.0);
        bins = function.partition();
    }

    /**
     * Test method for {@link io.cloudex.framework.partition.entities.Partition#joinPartitionItems(java.util.List)}.
     */
    @Test
    public void testJoinPartitionItems() {
        assertEquals(5, bins.size());
        
        List<String> parts = Partition.joinPartitionItems(bins);
        
        int i = 0;
        for(Partition bin: bins) {
            List<String> joined = new ArrayList<>();
            
            for(Item item: bin.getItems()) {
                joined.add(item.getKey());
            }
            
            assertEquals(parts.get(i), StringUtils.join(joined, ','));
            
            i++;
            
        }
    }

}
