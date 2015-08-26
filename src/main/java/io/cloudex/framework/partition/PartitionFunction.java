/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
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

package io.cloudex.framework.partition;

import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;

import java.util.List;

/**
 * Common interface for a partitioning function. The function is responsible for partitioning a number
 * of items into partitions.
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface PartitionFunction {
    
    public String ITEMS_KEY = "items";

    /**
     * the items to partition
     * @param items
     */
    public void setItems(List<? extends Item> items);

    /**
     * partition the items based on the partitioning function used
     * @return
     */
    public List<Partition> partition();
    

}
