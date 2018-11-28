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
    private static int M = 100;
    private static Grid grid;
    private static Map<String, Long> mapCategory;
    private static Map<String, Long> mapCategories;
    private static Map<String, Long> mapVenues;

    public static ArrayList<Row> LoadVertices(String pathCatType, String pathVenueCategory, String pathVenues) {

        int offset = 0;

        ArrayList<Row> vertList = new ArrayList<>();

        Utils.setVenuesPath(pathVenues);
        Utils.setCategoriesPath(pathVenueCategory);
        Utils.setCategoryPath(pathCatType);

        System.out.println("Los vertices se cargan de los archivos...");
        System.out.println("category: " + Utils.categoryPath);
        System.out.println("categories: " + Utils.categoriesPath);
        System.out.println("venues: " + Utils.venuesPath);
        System.out.println("\n\n");

        mapCategory = fillCategory(0, vertList, Utils.categoryPath);
        offset += mapCategory.size();
        //System.out.println("mapCategory: " + mapCategory);

        mapCategories = fillCategories(offset, vertList, Utils.categoriesPath);
        offset += mapCategories.size();
        //System.out.println("mapCategories: " + mapCategories);

        mapVenues = fillVenues(offset, vertList, Utils.venuesPath);
        offset += mapVenues.size();
        //System.out.println("mapVenues: " + mapVenues);

        return vertList;
    }


    private static Map<String, Long> fillCategory(long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapCattype = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(int i = 1; i < arr.length; i++){

                String data = (String) arr[i];
                data = data.replace("\"", "");

                vertList.add(RowFactory.create(i,null,null,null,null,null,data,6));
                mapCattype.put( data, i+offset);
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategory + " + e);
            e.printStackTrace();
        }

        return mapCattype;
    }

    private static Map<String, Long> fillCategories(long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapVenueCategory = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(int i = 1; i < arr.length; i++){
                String data = (String) arr[i];
                String datas[] = data.split(",");
                String finalData = datas[0].replace("\"", "");

                mapVenueCategory.put(finalData, i+offset);
                vertList.add(RowFactory.create(i,null,null,null,null,finalData,null,4));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategories + " + e);
            e.printStackTrace();
        }

        return mapVenueCategory;
    }

    private static Map<String, Long> fillVenues(long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapVenues = new HashMap<>();
        Set<Venue> venues = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(int i = 1 ; i < arr.length; i++){
                String data = (String) arr[i];
                String datas[] = data.split(",");
                Long latitude = Long.parseLong(datas[2].replace("\"", ""));
                Long longitude = Long.parseLong(datas[3].replace("\"", ""));
                String id = datas[0].replace("\"", "");

                venues.add(new Venue(id, latitude, longitude));
                mapVenues.put(id, i+offset);
                vertList.add(RowFactory.create(i,null,null,null,id,null,null,2));
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

    public static Grid getGrid() {
        return grid;
    }
}
