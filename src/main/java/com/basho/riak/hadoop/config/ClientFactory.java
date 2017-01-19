/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.hadoop.config;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakException;

/**
 * Used for generating clients for input/output
 * 
 * @author russell
 * 
 */
public final class ClientFactory {

    private ClientFactory() {}

    public static RiakClient getClient(RiakLocation location) throws RiakException {
        try {
            RiakClient client = RiakClient.newClient(location.getPort(), location.getHost());
            return client;
        } catch (UnknownHostException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Generate a cluster client from an array of {@link RiakLocation}s
     * 
     * @param riakLocations
     * @return
     * @throws IllegalArgumentException
     *             if locations are not all of same {@link RiakTransport}
     */
    public static RiakClient clusterClient(RiakLocation[] riakLocations) throws RiakException {
        if (riakLocations == null || riakLocations.length == 0) {
            throw new RiakException("No locations");
        }
        try {
            RiakClient client = RiakClient.newClient(riakLocations[0].getPort(),
                    Arrays.stream(riakLocations)
                            .map(riakLocation -> riakLocation.getHost())
                            .collect(Collectors.toList()).toArray(new String[0]));
            return client;
        } catch (UnknownHostException e) {
            throw new RiakException(e);
        }
    }
}
