package org.simple.store;


import java.util.Set;

public class DirMeta {
    public DirMeta(Set subdir, long lastModified){
        this.subdir= subdir;
        this.lastModified= lastModified;
    }

    public final long lastModified;
    public final Set<String> subdir;
}
