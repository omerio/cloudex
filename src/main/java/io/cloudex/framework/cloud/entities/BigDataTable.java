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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a table in a Big Data cloud provider service
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BigDataTable {
    
    private String name;
    
    private List<BigDataColumn> columns;
    

    /**
     * @param name - the name of the table
     */
    public BigDataTable(String name) {
        super();
        this.name = name;
    }

    /**
     * @param name - the name of the table
     * @param columns - the table columns schema
     */
    public BigDataTable(String name, List<BigDataColumn> columns) {
        super();
        this.name = name;
        this.columns = columns;
    }
    
    /**
     * Add a cloumn to this table
     * @param name - the column name
     * @param type - the column type
     */
    public void addColumn(String name, String type) {
        if(this.columns == null) {
            this.columns = new ArrayList<>();
        }
        
        this.columns.add(new BigDataColumn(name, type));
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the columns
     */
    public List<BigDataColumn> getColumns() {
        return columns;
    }

    /**
     * @param columns the columns to set
     */
    public void setColumns(List<BigDataColumn> columns) {
        this.columns = columns;
    }
    
    

}
