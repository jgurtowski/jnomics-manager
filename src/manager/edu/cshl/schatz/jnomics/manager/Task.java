package edu.cshl.schatz.jnomics.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import edu.cshl.schatz.jnomics.tools.PairedEndLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: james
 */
public class Task {

    private static final Map<String,Class> functions =
            new HashMap<String, Class>(){
                {
                    put("loader-pairend", PairedEndLoader.class);
                }
            };

    private Gson gson;

    @Expose
    private String name;
    @Expose
    private String[] dependencies;
    @Expose
    private String[] arguments;
    @Expose
    private String error;

    public int launch() throws Exception{
        try{
            ManagerTask t = (ManagerTask)functions.get(name).newInstance();
            t.runTask(arguments);
            return 0;
        }catch(Exception e){
            error = e.toString();
            return -1;
        }
    }

    public static Task fromJson(String data){
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.fromJson(data,Task.class);
    }
    
    public Task(){
        this("",new String[]{},new String[]{});
    }

    public Task(String name, String []depend, String []args){
        this.name = name;
        dependencies = depend;
        arguments = args;
        this.error = null;
        gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    }

    public String toJson(){
        return gson.toJson(this);
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getDependencies() {
        return dependencies;
    }

    public void setDependencies(String[] dependencies) {
        this.dependencies = dependencies;
    }

    public String[] getArguments() {
        return arguments;
    }

    public void setArguments(String[] arguments) {
        this.arguments = arguments;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
