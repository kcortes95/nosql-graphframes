package loader;

import models.Stop;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadNewEdges {

    public static ArrayList<Row> LoadEdges(List<Map> list) {
        ArrayList<Row> edges = new ArrayList<Row>();


        String path0 = "";
        String path1 = "";
        String path2 = "";
        String path3 = "";
        
        loadStopStopEdges(list.get(3), LoadNewVertices.getStops(path0), edges, path1);
        loadStopVenuesEdges(list.get(3), list.get(2), edges, path1);
        loadVenuesCategoriesEdges(list.get(2), list.get(1), edges, path2);
        loadCategoriesCategoryEdges(list.get(1), list.get(0), edges, path3);

        return edges;
    }

    private static void loadStopStopEdges(Map<Stop, Long> stops, Map<String, List<Long>> all, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                //me
                String userid = datas[0];
                String utctimestamp = datas[1];
                String tpos = datas[2];

                Stop stop = new Stop(userid, utctimestamp, tpos);

                Long from = stops.get(stop);

                List<Long> listLong = all.get(userid);

                listLong.forEach( e -> {
                    Long to = e;
                    edges.add(RowFactory.create(from, to, true, false, false, false, 1));
                });

            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadStopVenuesEdges + " + e);
            e.printStackTrace();
        }
    }

    private static void loadStopVenuesEdges(Map<Stop, Long> stops, Map<String, Long> venues, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                String userid = datas[0];
                String utctimestamp = datas[1];
                String tpos = datas[2];

                String venueid = datas[3];

                Stop stop = new Stop(userid, utctimestamp, tpos);

                Long from = stops.get(stop);
                Long to = venues.get(venueid);

                edges.add(RowFactory.create(from, to, false, true, false, false, 3));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadStopVenuesEdges + " + e);
            e.printStackTrace();
        }
    }

    private static void loadVenuesCategoriesEdges(Map<Stop, Long> venues, Map<String, Long> categories, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                String venueid = datas[0];
                String category = datas[1];

                Long from = venues.get(venueid);
                Long to = categories.get(category);

                edges.add(RowFactory.create(from, to, false, false, true, false, 5));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadVenuesCategoriesEdges + " + e);
            e.printStackTrace();
        }
    }

    private static void loadCategoriesCategoryEdges(Map<Stop, Long> categories, Map<String, Long> category, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                String cats = datas[0];
                String cat = datas[1];

                Long from = categories.get(cats);
                Long to = category.get(cat);

                edges.add(RowFactory.create(from, to, false, false, false, true, 7));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadCategoriesCategoryEdges + " + e);
            e.printStackTrace();
        }
    }



}
