package io.pravega.client.stream.impl;

import io.pravega.client.stream.Stream;

public abstract class StreamInternal implements Stream {

    /**
     * Gets the scoped name of this stream.
     *
     * @return String a fully scoped stream name
     */
    @Override
    public String getScopedName() {
        StringBuffer sb = new StringBuffer();
        String scope = getScope();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(getStreamName());
        return sb.toString();
    }
    
    public static Stream fromScopedName(String scopedName) {
        String[] tokens = scopedName.split("/");
        if (tokens.length == 2) {
            return new StreamImpl(tokens[0], tokens[1]);
        } else {
            throw new IllegalArgumentException("Not a valid segment name");
        }
    }
    
}
