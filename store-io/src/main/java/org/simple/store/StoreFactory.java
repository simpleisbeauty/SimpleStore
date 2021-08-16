package org.simple.store;

public interface StoreFactory {

    Store getStore(String name);

    void createStore(String name);
    void deleteStore(String name);

}
