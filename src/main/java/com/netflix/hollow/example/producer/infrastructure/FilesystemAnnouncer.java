/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.example.producer.infrastructure;

import com.netflix.hollow.example.producer.Announcer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FilesystemAnnouncer implements Announcer {

    public static final String ANNOUNCEMENT_FILENAME = "announced.version";
    
    private final File publishDir;

    public FilesystemAnnouncer(File publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public void announce(long stateVersion) {
        File announceFile = new File(publishDir, ANNOUNCEMENT_FILENAME);
        
        try (FileWriter writer = new FileWriter(announceFile)){
            writer.write(String.valueOf(stateVersion));
        } catch(IOException ex) {
            throw new RuntimeException("Unable to write to announcement file", ex);
        }
    }
}
