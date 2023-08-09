package org.course.utils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvReader {
    List<Map<String, String>> data;

    public void read (String path) throws IOException {
        File csvFile = new File(path);
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader(); // use first row as header; otherwise defaults are fine
        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(schema)
                .readValues(csvFile);

        data = it.readAll();
        //System.out.println(list);
    }

    public Map<String, String> getFirstMatch(String column, String value){

        for (Map<String, String> row: data) {
            //Si encuentra el valor en la columna en, regresa la primera columna donde encontro
            //System.out.println("rowasmap: " + row.get(column));
            if ( row.get(column).equals(value)) {
                return row;
            }
        }
            return Collections.emptyMap();
    }

    public Map<String, String> getRowById(Integer rowId){
        return data.get(rowId);
    }

    public Integer getCsvLength(){
        return data.size();
    }

}
