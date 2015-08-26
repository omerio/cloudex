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

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Item represent an item that has a weight and can be partitioned into containers.
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Item implements Comparable<Item>   {

    private String key;

    /**
     * The weight of this item
     */
    private Long weight;

    /**
     * The scale of this item compared to another item
     */
    private Double scale;

    /**
     * 
     */
    public Item() {
        super();
    }

    /**
     * @param key
     * @param weight
     */
    public Item(String key, Long weight) {
        super();
        this.key = key;
        this.weight = weight;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public Item setKey(String key) {
        this.key = key;
        return this;
    }

    /**
     * @return the weight
     */
    public Long getWeight() {
        return weight;
    }

    /**
     * @param weight the weight to set
     */
    public Item setWeight(Long weight) {
        this.weight = weight;
        return this;
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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("key", key)
            .append("weight", weight)
            .append("scale", scale)
            .toString();
    }

    @Override
    public int compareTo(Item item) {
        return this.weight.compareTo(item.weight);
    }


}
