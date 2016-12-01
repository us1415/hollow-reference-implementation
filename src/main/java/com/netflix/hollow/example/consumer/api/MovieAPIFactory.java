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
package com.netflix.hollow.example.consumer.api;

import com.netflix.hollow.api.client.HollowAPIFactory;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.api.objects.provider.HollowFactory;
import com.netflix.hollow.core.read.dataaccess.HollowDataAccess;
import com.netflix.hollow.example.consumer.api.generated.MovieAPI;
import java.util.Collections;

public class MovieAPIFactory implements HollowAPIFactory {

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess) {
        return new MovieAPI(dataAccess);
    }

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess, HollowAPI previousCycleAPI) {
        return new MovieAPI(dataAccess, Collections.<String>emptySet(), Collections.<String, HollowFactory<?>>emptyMap(), (MovieAPI) previousCycleAPI);
    }

}
