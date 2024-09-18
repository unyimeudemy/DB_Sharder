package com.piraxx.sharder.sharderPackage;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {

    private int numberOfReplicas;

/**   By using a TreeMap, nodes and keys are efficiently
 * distributed across the ring, and the size of the hash
 * space is effectively infinite (limited only by the size of the hash
 * function's output, e.g., 32 bits, 64 bits). Meaning that the "size"
 * of the hash ring is determined by the hash function.
 *
 * Since we are using MD5, it will produce a 128-bit hash. And we
 * are using just a portion of that hash to map nodes
 * and keys to an integer in a specific range (in this case,
 * up to the maximum value of a 32-bit signed integer).
 */
    private static final SortedMap<Integer, String> circle = new TreeMap<>();

    public ConsistentHashing() {
        setReplicas();
    }

    private void setReplicas(){
        this.numberOfReplicas = Integer.parseInt(System.getenv("replicas"));
    }

    public void addNode(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = hash(node + i);
            circle.put(hash, node);
        }
    }

    public void removeNode(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = hash(node + i);
            circle.remove(hash);
        }
    }

    /**
     * Another feature of the data structure is it ability to return
     * a subsection of the map based on any key value provided. The subsection returned is starts
     * from the provided key to the end of the map. The data structure
     * gets very beautiful when an invalid key within the
     * range of the map’s key set stills returns something.
     * This is because in an event that an invalid key is provided,
     * a subsection starting from the key entry that ]
     * is just greater than the invalid key is returned.
     *
     * With this being returned, we can just use the `firstKey` method to get the next
     * valid key which now happens to be the next  node
     * going clockwise assuming the map is considered as a ring.
     */
    public String getNode(Object keyObj) {
        String key = keyObj.toString();
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hash(key);
        SortedMap<Integer, String> tailMap = circle.tailMap(hash);
        int nodeHash = !tailMap.isEmpty() ? tailMap.firstKey() : circle.firstKey();
        return circle.get(nodeHash);
    }



    private int hash(Object keyObj) {
        try {
            String key = keyObj.toString();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));

            return Math.abs(
                    digest[0] << 24 |
                            (digest[1] & 0xFF) << 16 |
                            (digest[2] & 0xFF) << 8 |
                            (digest[3] & 0xFF)
            );
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No such algorithm exception", e);
        }
    }

    public SortedMap<Integer, String> getHashRing(){
        return circle;
    }
}

