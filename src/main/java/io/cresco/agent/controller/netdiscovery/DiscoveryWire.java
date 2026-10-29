package io.cresco.agent.controller.netdiscovery;

import io.netty.handler.codec.serialization.ClassResolver;

/**
 * The discovery wire carries Java-serialized Strings (JSON inside). Netty's ObjectDecoder would deserialize any class a
 * peer names -- before any authentication, from anyone who can reach the port. Only String is resolved here; anything
 * else fails the decode and the connection is closed. Old peers send Strings too, so the wire is unchanged.
 */
final class DiscoveryWire {

    private DiscoveryWire() { }

    static final int MAX_OBJECT_BYTES = 1024 * 1024;

    static final ClassResolver STRINGS_ONLY = className -> {
        if ("java.lang.String".equals(className)) return String.class;
        throw new ClassNotFoundException("discovery accepts only java.lang.String, not " + className);
    };
}
