package loader;

import models.Stop;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadNewVertices {

    static Map<String, Long> mapCattype;
    static Map<String, Long> mapVenueCategory;
    static Map<String, Long> mapVenues;
    static Map<Stop, Long> mapStops;

    public static ArrayList<Row> LoadVertices(String pathCatType, String pathVenueCategory, String pathVenues, String pathStops) {

        //pathCatType = "/home/kcortesrodrigue/categories.csv";
        //pathVenueCategory = "/home/kcortesrodrigue/venuecategory.csv";
        //pathVenues = "/home/kcortesrodrigue/venueid.csv";
        //pathStops = "/home/kcortesrodrigue/stops.csv";

        long offset = 0L;

        ArrayList<Row> vertList = new ArrayList<>();

        mapCattype = fillCatType(0L, vertList, pathCatType);
        offset += mapCattype.size();

        mapVenueCategory = fillVenueCategory(offset, vertList, mapCattype, pathVenueCategory);
        offset += mapVenueCategory.size();

        mapVenues = fillVenues(offset, vertList, mapVenueCategory, pathVenues);
        offset += mapVenues.size();

        mapStops = fillStops(offset, vertList, mapVenues, pathStops);

        return vertList;
    }


    private static Map<String, Long> fillCatType(Long offset, ArrayList<Row> vertList, String path) {

        Map<String, Long> mapCattype = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                vertList.add(RowFactory.create(i,null,null,null,null,null,arr[(int)i],6));
                mapCattype.put( (String) arr[(int)i], i);
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillCatType + " + e);
            e.printStackTrace();
        }

        return mapCattype;
    }

    private static Map<String, Long> fillVenueCategory(Long offset, ArrayList<Row> vertList, Map<String, Long> mapCattype, String path) {

        Map<String, Long> mapVenueCategory = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");
                mapVenueCategory.put( datas[0], i);
                vertList.add(RowFactory.create(i,null,null,null,null,datas[0],null,4));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillVenueCategory + " + e);
            e.printStackTrace();
        }

        return mapVenueCategory;
    }

    private static Map<String, Long> fillVenues(Long offset, ArrayList<Row> vertList, Map<String, Long> mapVenueCategory, String path) {

        Map<String, Long> mapVenues = new HashMap<>();

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1 + offset ; i < arr.length + offset; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");
                mapVenues.put( datas[0], i);
                vertList.add(RowFactory.create(i,null,null,null,datas[0],null,null,2));
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
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");
                Stop stop = new Stop(datas[0], datas[1], datas[2]);
                map.put(stop, i);
                vertList.add(RowFactory.create(i,datas[0],datas[1],datas[2],null,null,null,0));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la fillStops + " + e);
            e.printStackTrace();
        }

        return map;

    }

    public static List<Map> getMaps(){
        List<Map> list = new ArrayList<>();
        list.add(mapCattype);
        list.add(mapVenueCategory);
        list.add(mapVenues);
        list.add(mapStops);
        return list;
    }


}
