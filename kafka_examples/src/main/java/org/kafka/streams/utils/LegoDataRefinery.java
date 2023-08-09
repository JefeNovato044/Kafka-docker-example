package org.kafka.streams.utils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.course.utils.CsvReader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LegoDataRefinery {
    static CsvReader reader = new CsvReader();

    public LegoDataRefinery(String catFilePath) throws IOException {
        this.reader.read(catFilePath);
    }

    public String refine(String originalData) {
        Gson gson = new Gson();

        //Parsear string a json
        JsonObject jsonObject = null;
        JsonObject jsonObjectOut = new JsonObject();
        try {
            jsonObject = gson.fromJson(originalData, JsonObject.class);
            System.out.println("Dato parseado: " + jsonObject);
        } catch (JsonSyntaxException e) {
            System.err.println("Error al parsear dato: " + e.getMessage());
            return "";
        }


        String id_set = jsonObject.get("set_id").getAsString();
        Float precio = jsonObject.get("price").getAsFloat();

        Map<String, String> row = reader.getFirstMatch("prod_id", id_set+".0");
        System.out.println(id_set);
        System.out.println(row);

        //
        jsonObjectOut.addProperty("id", row.get("prod_id") );

        //Agregar nombre del set
        jsonObjectOut.addProperty("set_name", row.get("set_name") );

        //Agregar la edad recomendada

        jsonObjectOut.addProperty("age_range", row.get("ages") );

        //Precio recomendado
        jsonObjectOut.addProperty("recommended_price", row.get("list_price") );
        //Calcular sobreprecio (ganancia)

        //Precio vendido

        //Tema del set


        return jsonObjectOut.toString();
    }

}
