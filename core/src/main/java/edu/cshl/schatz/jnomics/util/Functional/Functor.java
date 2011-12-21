package edu.cshl.schatz.jnomics.util.Functional;

import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

/**
 * User: james
 * Functions
 */
public class Functor {

     public static <T> void apply(T[] data, Operation operation) throws Exception{
         for(T i: data){
             operation.performOperation(i);
         }
     }

    public static <T,R> R[] applyAndReturn(T[] data, Class returnType, Operation operation) throws Exception{

        R[] newArray = (R[])Array.newInstance(returnType, data.length);
        int i=0;
        for(T d: data){
            newArray[i++] = operation.performAndReturnOperation(d);
        }
        return newArray;
    }
}
