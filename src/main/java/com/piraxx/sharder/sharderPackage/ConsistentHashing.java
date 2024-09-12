package com.piraxx.sharder.sharderPackage;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {

    private final int numberOfReplicas;
    private final SortedMap<Integer, String> circle = new TreeMap<>();

    public ConsistentHashing(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
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
}

