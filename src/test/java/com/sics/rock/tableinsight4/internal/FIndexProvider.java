package com.sics.rock.tableinsight4.internal;

import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FIndexProvider<T> implements Serializable {

    private Map<T, Integer> indexes = new HashMap<>();

    private List<T> objects = new ArrayList<>();

    private int nextIndex = 0;

    public void put(T object) {
        FAssertUtils.require(() -> !contain(object), () -> object + " has already put in " + objects);
        getIndex(object);
    }

    public int getIndex(T object) {
        Integer index = indexes.putIfAbsent(object, nextIndex);
        if (index == null) {
            index = nextIndex;
            ++nextIndex;
            objects.add(object);
        }
        return index;
    }

    public T get(int index) {
        FAssertUtils.require(() -> index < objects.size(), () -> objects + " has not index " + index);
        return objects.get(index);
    }

    public List<T> getAll() {
        return objects;
    }

    public boolean contain(T object) {
        return indexes.containsKey(object);
    }

    @Override
    public String toString() {
        return objects.toString();
    }

    public int size() {
        return objects.size();
    }
}
