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

package io.cloudex.framework.partition.builtin;

import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.partition.PartitionUtils;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.Validate;

/**
 * A variation of the Bin Packing algorithm
 * 1- First fit
 * 2- Ordered first fit (First fit decreasing)
 * 3- Full Bin
 * 
 * This class also supports the setting of number of bins before. Used in scenarios where
 * a number of predefined bins should be used and we don't care about capacity.
 * 
 * This function needs the following keys set on the input
 * 
 * <ul>
 * <li>newBinPercentage - Double</li>
 * <li>maxBinItems - Long</li>
 * <li>numberOfBins - Integer</li>
 * </ul>
 * 
 * For more information about bin packing problems see: http://mathworld.wolfram.com/Bin-PackingProblem.html
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BinPackingPartition implements PartitionFunction {

    /*public static final String NEW_BIN_PERCENTAGE = "newBinPercentage";
    public static final String MAX_BIN_ITEMS = "maxBinItems";
    public static final String NUMBER_OF_BINS = "numberOfBins";*/

    private List<? extends Item> items;// = new ArrayList<>();

    private Long maxBinItems;

    private Double newBinPercentage = 0.5;

    private Integer numberOfBins;


    /**
     * 
     */
    public BinPackingPartition() {
        super();
    }

    /**
     * @param items - the items to partition
     */
    public BinPackingPartition(List<Item> items) {
        super();
        this.items = items;
    }


    /* (non-Javadoc)
     * @see io.cloudex.framework.partition.PartitionFunction#setItems(java.util.List)
     */
    @Override
    public void setItems(List<? extends Item> items) {
        this.items = items;
    }

    @Override
    public List<Partition> partition() {

        Validate.notNull(this.items);

        // sort descending
        Collections.sort(items, Collections.reverseOrder());

        // have we got a maximum set?, otherwise use the size of the largest item
        Long max = (this.maxBinItems != null) ? this.maxBinItems : items.get(0).getWeight();

        PartitionUtils.setScale(items, max);

        long sum = PartitionUtils.sum(items, max);

        // check if the number of bins have already been set, in which case we have to fit everything in them
        if(this.numberOfBins == null) {
            // lower bound number of bin
            double numBins = (sum / (float) max);
            double numWholeBins = Math.floor(numBins);
            this.numberOfBins = (int) numWholeBins;
            double excess = numBins - numWholeBins;
            if(excess > newBinPercentage) {
                this.numberOfBins++;
            }

        } else {
            max = (long) Math.ceil(sum / (float) this.numberOfBins);
        }

        List<Partition> bins = new ArrayList<>();
        
        if(this.numberOfBins == 1) {
            
            Partition bin = new Partition();
            bins.add(bin);
            bin.addAll(items);
            items.clear();

        } else {

            // Mix of First Fit Decreasing (FFD) strategy and Full Bin strategy
            for(int i = 0; i < this.numberOfBins; i++) {
                Partition bin = new Partition();
                bins.add(bin);

                // check if we have ran out of items
                if(!items.isEmpty()) {

                    Item largestItem = items.get(0);

                    bin.add(largestItem);
                    items.remove(largestItem);

                    Long currentSum = bin.sum();

                    if(currentSum < max) {
                        Long diff = max - currentSum;
                        List<Item> toRemove = new ArrayList<>();

                        for(Item item: items) {
                            //Item item = items.get(j);
                            if(item.getWeight() <= diff) {

                                bin.add(item);
                                toRemove.add(item);

                                if(item.getWeight() == diff) {
                                    break;
                                } else {
                                    // look for an even small number to fill the gap
                                    diff = max - bin.sum();
                                }
                            } 
                        }

                        items.removeAll(toRemove);

                    }
                }
                bin.calculateScale();
            }

            /*if(!items.isEmpty()) {
            bins.get(bins.size() - 1).addAll(items);
            items.clear();
        }*/

            // spread out remaining items, this approximate
            if(!items.isEmpty()) {
                //bins.get(bins.size() - 1).addAll(items);
                //items.clear();

                // items are in descending order
                // sort partitions in ascending order
                Collections.sort(bins);

                Partition smallest;
                long largestSum = bins.get(bins.size() - 1).sum();
                int index = 0;
                do {

                    smallest = bins.get(index);

                    // spread the remaining items into the bins, largest item into smallest bin
                    for(int i = 0; i < items.size(); i++) {

                        smallest.add(items.remove(i));

                        if(smallest.sum() > largestSum) {
                            break;
                        }
                    }

                    index++;

                } while (!items.isEmpty());

                items.clear();
            }

        }

        return bins;

    }


    /**
     * @param numberOfBins the numberOfBins to set
     */
    public void setNumberOfBins(Integer numberOfBins) {
        this.numberOfBins = numberOfBins;
    }


    /**
     * @param maxBinItems the maxBinItems to set
     */
    public void setMaxBinItems(Long maxBinItems) {
        this.maxBinItems = maxBinItems;
    }

    /**
     * @param newBinPercentage the newBinPercentage to set
     */
    public void setNewBinPercentage(Double newBinPercentage) {
        this.newBinPercentage = newBinPercentage;
    }

    /**
     * @return the items
     */
    public List<? extends Item> getItems() {
        return items;
    }

    /**
     * @return the maxBinItems
     */
    public Long getMaxBinItems() {
        return maxBinItems;
    }

    /**
     * @return the newBinPercentage
     */
    public Double getNewBinPercentage() {
        return newBinPercentage;
    }

    /**
     * @return the numberOfBins
     */
    public Integer getNumberOfBins() {
        return numberOfBins;
    }

}
