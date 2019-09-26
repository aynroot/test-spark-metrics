package io.relayr.helpers;

import java.io.Serializable;

public class Lazy<T extends Serializable> implements Serializable {
    private T instance = null;
    private SerializableSupplier<T> supplier;

    public Lazy(SerializableSupplier<T> theSupplier) {
        supplier = theSupplier;
    }

    public T get() {
        if (instance == null) {
            instance = supplier.get();
        }

        return instance;
    }
}