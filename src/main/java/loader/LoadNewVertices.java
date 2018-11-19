package loader;

import models.Stop;
import models.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadNewVertices {

    static Map<String, Long> mapCategory;
    static Map<String, Long> mapCategories;
    static Map<String, Long> mapVenues;
    static Map<Stop, Long> mapStops;

    public static ArrayList<Row> LoadVertices(String pathCatType, String pathVenueCategory, String pathVenues, String pathStops) {

        long offset = 0L;

        ArrayList<Row> vertList = new ArrayList<>();

        mapCategory = fillCategory(0L, vertList, Utils.categoryPath);
        offset += mapCategory.size();
        //System.out.println("mapCategory: " + mapCategory);

        mapCategories = fillCategories(offset, vertList, mapCategory, Utils.categoriesPath);
        offset += mapCategories.size();
        //System.out.println("mapCategories: " + mapCategories);

        mapVenues = fillVenues(offset, vertList, mapCategories, Utils.venuesPath);
        offset += mapVenues.size();
        //System.out.println("mapVenues: " + mapVenues);

        mapStops = fillStops(offset, vertList, mapVenues, Utils.stopPath);
        //System.out.println("mapStops: " + mapStops);

        return vertList;
    }


    private static Map<String, Long> fillCategory(Long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapCattype = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){

                String data = (String) arr[(int)i];
                data = data.replace("\"", "");

                vertList.add(RowFactory.create(i,null,null,null,null,null,data,6));
                mapCattype.put( data, i);
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategory + " + e);
            e.printStackTrace();
        }

        return mapCattype;
    }

    private static Map<String, Long> fillCategories(Long offset, ArrayList<Row> vertList, Map<String, Long> mapCattype, String path) {

        Map<String, Long> mapVenueCategory = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)(i - offset)];
                String datas[] = data.split(",");
                String finalData = datas[0].replace("\"", "");

                mapVenueCategory.put(finalData, i);
                vertList.add(RowFactory.create(i,null,null,null,null,finalData,null,4));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCategories + " + e);
            e.printStackTrace();
        }

        return mapVenueCategory;
    }

    private static Map<String, Long> fillVenues(Long offset, ArrayList<Row> vertList, Map<String, Long> mapVenueCategory, String path) {

        Map<String, Long> mapVenues = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)(i - offset)];
                String datas[] = data.split(",");
                String finalData = datas[0].replace("\"", "");

                mapVenues.put(finalData, i);
                vertList.add(RowFactory.create(i,null,null,null,finalData,null,null,2));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillVenues + " + e);
            e.printStackTrace();
        }

        return mapVenues;
    }

    private static Map<Stop, Long> fillStops(Long offset, ArrayList<Row> vertList, Map<String, Long> mapVenues, String path) {

        Map<Stop, Long> map = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)(i - offset)];
                String datas[] = data.split(",");
                Stop stop = new Stop(datas[0], datas[2], datas[3]);
                map.put(stop, i);

                SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                String dateString = datas[2];
                dateString = dateString.replace("'", "");
                java.sql.Date d = new java.sql.Date(sdf.parse(dateString).getTime());
                String userId = datas[0].replace("'", "");
                Long tpos = Long.parseLong(datas[3]);

                vertList.add(RowFactory.create(i,userId,d,tpos,null,null,null,0));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillStops + " + e);
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("Tiro exception en ParseException + " + e);
            e.printStackTrace();
        }

        return map;

    }

    public static List<Map> getMaps(){
        List<Map> list = new ArrayList<>();
        list.add(mapCategory);
        list.add(mapCategories);
        list.add(mapVenues);
        list.add(mapStops);
        return list;
    }

    public static Map<String, List<Long>> getStops(String path) {

        Map<String, List<Long>> map = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");
                String userid = datas[0].replace("'","");

                if (!map.containsKey(userid)){
                    map.put(userid, new ArrayList<>());
                }

                List<Long> l = map.get(userid);
                l.add(i);
                map.put(userid, l);

            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillStops + " + e);
            e.printStackTrace();
        }

        return map;

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

    public static Map<Stop, Long> getStops() {
        return mapStops;
    }
}
