package com.neu;

import org.apache.mahout.cf.taste.impl.common.FastByIDMap;

public class storeIndexedData {

    private final FastByIDMap<String> longToString;

    protected storeIndexedData() {
        this.longToString = new FastByIDMap<String>(100);
    }

    public void storeMappingInMemory(long longID, String stringID) {
        synchronized (longToString) {
            longToString.put(longID, stringID);
        }
    }

    public String toStringID(long longID) {
        synchronized (longToString) {
            return longToString.get(longID);
        }
    }

    public FastByIDMap<String> getIndexedMap(){

        return longToString;
    }

}
