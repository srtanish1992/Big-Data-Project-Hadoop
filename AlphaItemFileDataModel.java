package com.neu;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;

import java.io.File;
import java.io.IOException;

public class AlphaItemFileDataModel extends FileDataModel {

    private static final long serialVersionUID = 1L;
    indexGenerator indexgenerator;


    public AlphaItemFileDataModel(File dataFile) throws IOException {
        super(dataFile);
    }

    @Override
    protected long readItemIDFromString(String value) {
        if(indexgenerator == null){
            indexgenerator= new indexGenerator();
        }
        return Long.parseLong(indexgenerator.toLongID(value)+"");
    }

    @Override
    protected long readUserIDFromString(String value) {
        if(indexgenerator == null){
            indexgenerator= new indexGenerator();
        }
        return Long.parseLong(indexgenerator.toLongID(value)+"");
    }

    public FastByIDMap<String> getIndexedMapInstansce(){
        return indexgenerator.getIndexedMapInstansce();
    }

}
