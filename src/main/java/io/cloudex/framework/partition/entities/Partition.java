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

package io.cloudex.framework.partition.entities;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A partition is a collection of items.
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Partition {
    
    private static final Log log = LogFactory.getLog(Partition.class);

    /**
     * The scale of this partition compared to another partition
     */
    private Double scale;

    /**
     * The items in this partition
     */
    private List<Item> items = new ArrayList<>();

    /**
     * Delegate to items.add
     * @param item
     */
    public void add(Item item) {
        this.items.add(item);
    } 

    /**
     * Delegate to items.addAll
     * @param items
     */
    public void addAll(List<? extends Item> items) {
        this.items.addAll(items);
    }

    /**
     * @return
     * @see java.util.List#size()
     */
    public int size() {
        return items.size();
    }

    /**
     * @param index
     * @return
     * @see java.util.List#get(int)
     */
    public Item get(int index) {
        return items.get(index);
    }

    /**
     * Set the scale for the parition
     */
    public void calculateScale() {

        this.scale = 0.0d;
        if(!this.items.isEmpty()) {
            for(Item item: this.items) {
                this.scale += item.getScale();
            }

            this.scale = this.scale / this.items.size();
        }
    }

    /**
     * Sums item weights
     * @param items
     * @return
     */
    public Long sum() {
        long sum = 0;
        for(Item item: items) {
            sum += item.getWeight();
        }
        return sum;
    }

    /**
     * @return the items
     */
    public List<Item> getItems() {
        return items;
    }

    /**
     * @param items the items to set
     */
    public void setItems(List<Item> items) {
        this.items = items;
    }

    /**
     * @return the scale
     */
    public Double getScale() {
        return scale;
    }

    /**
     * @param scale the scale to set
     */
    public void setScale(Double scale) {
        this.scale = scale;
    }

    /**
     * Return a list of comma separated items for each bin
     * @return a list representation of each partition
     */
    public static List<String> joinPartitionItems(List<Partition> bins) {
        List<String> binItems = null;
        if(bins.size() > 0) {
            binItems = new ArrayList<>();

            Long totalWeight = 0L;
            for(Partition bin: bins) {

                List<String> files = new ArrayList<>();
                Long binWeight = 0L;
                for(Item item: bin.getItems()) {
                    files.add(item.getKey());
                    binWeight += item.getWeight();

                }

                Double scale = (bin.size() == 1) ? bin.get(0).getScale() : 1d;

                log.info("Bin weight: " + binWeight + ", bin items: " + bin.size() + ", scale: " + scale);

                totalWeight += binWeight;

                String itemsStr = StringUtils.join(files, ','); 
                binItems.add(itemsStr);

            }
            log.info("Total weight of all items: " + totalWeight);
        }

        return binItems;
    }


    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
        .append("scale", scale)
        .append("items", items)
        .toString();
    }


}
