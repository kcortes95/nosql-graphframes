package loader;

import cellindexmethod.Grid;
import cellindexmethod.LinearGrid;
import models.Utils;
import models.Venue;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class LoadNewVertices {

    // M --> cuantas celdas quiero
    // Area de + - 50km x 50km
    private static int M = 250;
    private static Grid grid;

    private static Map<String, Long> mapCategory;
    private static Map<String, Long> mapCategories;
    private static Map<String, Long> mapVenues;
    private static Map<String, Venue> idToVenue;

    public static ArrayList<Row> LoadVertices(String pathCatType, String pathVenueCategory, String pathVenues) {

        long offset = 0;

        ArrayList<Row> vertList = new ArrayList<>();

        Utils.setVenuesPath(pathVenues);
        Utils.setCategoriesPath(pathVenueCategory);
        Utils.setCategoryPath(pathCatType);

        System.out.println("Los vertices se cargan de los archivos...");
        System.out.println("category: " + Utils.categoryPath);
        System.out.println("categories: " + Utils.categoriesPath);
        System.out.println("venues: " + Utils.venuesPath);
        System.out.println("\n\n");

        /*
        mapCategory = fillCategory(0L, vertList, Utils.categoryPath);
        offset += mapCategory.size();
        */

        /*
        mapCategories = fillCategories(offset, vertList, Utils.categoriesPath);
        //System.out.println(mapCategories);
        offset += mapCategories.size();
        */

        mapVenues = fillVenues(offset, vertList, Utils.venuesPath);

        return vertList;
    }


    private static Map<String, Long> fillCategory(Long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapCattype = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){

                String data = (String) arr[(int)i];
                data = data.replace("\"", "");

                vertList.add(RowFactory.create(i,null,null,data,6));
                mapCattype.put( data, i);
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategory + " + e);
            e.printStackTrace();
        }

        return mapCattype;
    }

    private static Map<String, Long> fillCategories(Long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapVenueCategory = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)(i - offset)];
                String datas[] = data.split(",");
                String finalData = datas[0].replace("\"", "");

                mapVenueCategory.put(finalData, i);
                vertList.add(RowFactory.create(i,null,finalData,null,4));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategories + " + e);
            e.printStackTrace();
        }

        return mapVenueCategory;
    }

    private static Map<String, Long> fillVenues(long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapVenues = new HashMap<>();
        idToVenue = new HashMap<>();
        Set<Venue> venues = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 ; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");
                Double latitude = Double.parseDouble(datas[2].replace("\"", ""));
                Double longitude = Double.parseDouble(datas[3].replace("\"", ""));
                String id = datas[0].replace("\"", "");

                Venue v = new Venue(id, latitude, longitude);
                venues.add(v);
                mapVenues.put(id, i+offset);
                idToVenue.put(id, v);
                vertList.add(RowFactory.create(i,id,null,null,2));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillVenues + " + e);
            e.printStackTrace();
        }

        grid = new LinearGrid(M, venues);
        return mapVenues;
    }

    public static Map<String, Long> getCategory() {
        return mapCategory;
    }

    public static Map<String, Long> getCategories() {
        return mapCategories;
    }

    public static Map<String, Long> getVenues() {
        return mapVenues;
    }

    public static Map<String, Venue> getIdToVenue() {
        return idToVenue;
    }

    public static Grid getGrid() {
        return grid;
    }
}
