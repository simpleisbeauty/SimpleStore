package org.simple.store;

public class ParentName {

    public ParentName(String parent, String name){
        this.parent= parent;
        this.name= name;
    }

    @Override
    public String toString(){
        return new StringBuilder("\"parent\":\"").append(parent).append("\", \"name\":\"").append(name).append('"').toString();
    }

    public String key(){
        return parent+ name;
    }

    public static String key(String parent, String name){
        return parent.endsWith("/")? parent+ name: parent+ '/'+ name;
    }

    public static String isDir(String dir){
        if(dir!= null && dir.charAt(dir.length()-1)== '/'){
            return dir;
        }
        else {
            throw new IllegalArgumentException(dir + " isn't end with /");
        }
    }

    public final String parent;
    public final String name;
}
