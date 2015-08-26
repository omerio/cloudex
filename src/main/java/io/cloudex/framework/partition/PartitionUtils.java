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

package io.cloudex.framework.partition;

import io.cloudex.framework.partition.entities.Item;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class PartitionUtils {

    /**
     * Sums a list of numbers, if a number is greater than max only max will be summed
     * @param items
     * @return
     */
    public static Long sum(List<? extends Item> items, long max) {
        long sum = 0;
        for(Item item: items) {
            Long weight = item.getWeight();
            if(weight > max) {
                weight = max;
            }
            sum += weight;
        }
        return sum;
    }

    /**
     * Set the scale of items based on the max provided
     * 1/2, 1, 2, 3, 4, 5, ..., n
     * @param max
     */
    public static List<? extends Item> setScale(List<? extends Item> items, long max) {
        for(Item item: items) {
            long weight = item.getWeight();
            Double scale = (double) weight / (double) max;
            if(scale >= 1d) {
                scale = (new BigDecimal(scale)).setScale(0, RoundingMode.HALF_UP).doubleValue();
            } else if(scale < 1d && scale > 0.5d) {
                scale = 1d;
            } else if(scale <= 0.5d) {
                scale = 0.5d;
            }

            item.setScale(scale);
        }
        return items;
    }

}
