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
package com.basho.riak.hadoop;

/**
 * @author russell
 * 
 */
public class RiakPBLocation extends RiakLocation {

    /**
     * @param host
     * @param port
     */
    protected RiakPBLocation(String host, int port) {
        super(RiakTransport.PB, host, port);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.RiakLocation#asString()
     */
    @Override public String asString() {
        return new StringBuilder(getHost()).append(":").append(getPort()).toString();
    }

}