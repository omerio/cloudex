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


package io.cloudex.framework.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class FileUtils {
    
    private static final Log log = LogFactory.getLog(FileUtils.class);

    public static final String PATH_SEPARATOR = File.separator;

    public static final String TEMP_FOLDER = System.getProperty("java.io.tmpdir") + PATH_SEPARATOR;

    public static final byte [] SEPARATOR = System.getProperty("line.separator").getBytes();
        
    public static final Gson GSON = new Gson();
    
    /**
     * The number of bytes in a kilobyte.
     */
    public static final long ONE_KB = 1024;

    /**
     * The number of bytes in a megabyte.
     */
    public static final long ONE_MB = ONE_KB * ONE_KB;

    /**
     * The number of bytes in a gigabyte.
     */
    public static final long ONE_GB = ONE_KB * ONE_MB;
    
    private static final int BUFFER = 1024 * 8;
    
    /**
     * Delete the file with the provided string
     * @param filename - the name of the file to delete
     * @return
     */
    public static boolean deleteFile(String filename) {
        File file = new File(filename);
        return file.delete();
    }
    
    /**
     * copy the file with the provided string
     * @param filename - the name of the file to copy
     * @throws IOException 
     */
    public static void copyFile(String filename, String newName) throws IOException {
        File file = new File(filename);
        Files.copy(file, new File(newName));
    }
    
    /**
     * Convert a json file to set
     * @param filename
     * @return
     * @throws IOException
     */
    public static Set<String> jsonFileToSet(String filename) throws IOException {
        
        try(FileReader reader = new FileReader(filename)) {
            
            return GSON.fromJson(new JsonReader(reader),  
                    new TypeToken<Set<String>>(){}.getType());
            
        } catch (Exception e) {
            log.error("failed to prase json into set", e);
            throw new IOException(e);
        }
    }
    
    /**
     * Convert a json file to map
     * @param filename
     * @return
     * @throws IOException
     */
    public static Map<String, Long> jsonFileToMap(String filename) throws IOException {
        
        try(FileReader reader = new FileReader(filename)) {
            
            return GSON.fromJson(new JsonReader(reader),  
                    new TypeToken<Map<String, Long>>(){}.getType());
            
        } catch (Exception e) {
            log.error("failed to prase json into map", e);
            throw new IOException(e);
        }
    }
    
    
    
    /**
     * 
     * @param filename
     * @param object
     * @throws IOException
     */
    public static void objectToJsonFile(String filename, Object object) throws IOException {
        try(FileWriter writer = new FileWriter(filename)) {
            GSON.toJson(object, writer);
        }
    }
    
    /**
     * Encode the provided text as filename
     * @param text
     * @return
     */
    public static String encodeFilename(String text) {
        String filename = text;
        try {
            filename = URLEncoder.encode(text, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            log.warn("Failed to encode text as filename: " + text, e);
        }
        return filename;
    }
    
    
    /**
     * (Gzip) Uncompress a compressed file
     * @param filename
     * @return
     * @throws IOException
     */
    public String unCompressFile(String filename) throws IOException {
        FileInputStream fin = new FileInputStream(filename);
        BufferedInputStream in = new BufferedInputStream(fin);
        String outFile = GzipUtils.getUncompressedFilename(filename);
        try(FileOutputStream out = new FileOutputStream(outFile);
                GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in)) {
            final byte[] buffer = new byte[BUFFER];
            int n = 0;
            while (-1 != (n = gzIn.read(buffer))) {
                out.write(buffer, 0, n);
            }
        }
        return outFile;
    }

}
