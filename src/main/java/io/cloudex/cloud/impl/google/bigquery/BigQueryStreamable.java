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


package io.cloudex.cloud.impl.google.bigquery;

import java.util.Map;

/**
 * Classes that can be streamed into Bigquery, each instance streamed as a BigQuery row.
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface BigQueryStreamable {
    
    /**
     * A Map object that contains a row of BigQuery data. The object's properties and values must
     * match the destination table's schema.
     * @return
     */
    public Map<String, Object> toMap();

}
