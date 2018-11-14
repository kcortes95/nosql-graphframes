package loader;

import models.Stop;
import models.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadNewEdges {

    public static ArrayList<Row> LoadEdges(List<Map> list) {
        ArrayList<Row> edges = new ArrayList<Row>();

        /*
        loadStopStopEdges(LoadNewVertices.getStops(), LoadNewVertices.getStops(Utils.stopPath), edges, Utils.stopPath);
        */

        System.out.println("en LOADEDGES: " + LoadNewVertices.getVenues());

        loadStopVenuesEdges(LoadNewVertices.getStops(), LoadNewVertices.getVenues(), edges, Utils.stopPath);
        loadVenuesCategoriesEdges(LoadNewVertices.getVenues(), LoadNewVertices.getCategories(), edges, Utils.venuesPath);
        loadCategoriesCategoryEdges(LoadNewVertices.getCategories(), LoadNewVertices.getCategory(), edges, Utils.categoriesPath);

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
                String utctimestamp = datas[2];
                String tpos = datas[3];

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
                String utctimestamp = datas[2];
                String tpos = datas[3];

                String venueid = datas[1];
                venueid = venueid.replace("'", "\"");

                Stop stop = new Stop(userid, utctimestamp, tpos);

                Long from = stops.get(stop);
                Long to = venues.get(venueid);

                System.out.println("-----");
                System.out.println( stop + " >> " + venueid);
                System.out.println( from + " >> " + to);
                System.out.println("-----");

                edges.add(RowFactory.create(from, to, false, true, false, false, 3));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadStopVenuesEdges + " + e);
            e.printStackTrace();
        }
    }

    private static void loadVenuesCategoriesEdges(Map<String, Long> venues, Map<String, Long> categories, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                String venueid = datas[0];
                String category = datas[1];

                Long from = venues.get(venueid);
                Long to = categories.get(category);

                //System.out.println( cats + "(" + from + ")" + " >> " + cat + "(" + to + ")");

                edges.add(RowFactory.create(from, to, false, false, true, false, 5));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadVenuesCategoriesEdges + " + e);
            e.printStackTrace();
        }
    }

    private static void loadCategoriesCategoryEdges(Map<String, Long> categories, Map<String, Long> category, ArrayList<Row> edges, String path){

        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            Object[] arr = stream.toArray();
            for(long i = 1; i < arr.length; i++){
                String data = (String) arr[(int)i];
                String datas[] = data.split(",");

                String cats = datas[0];
                String cat = datas[1];

                Long from = categories.get(cats);
                Long to = category.get(cat);

                //System.out.println( cats + "(" + from + ")" + " >> " + cat + "(" + to + ")");

                edges.add(RowFactory.create(from, to, false, false, false, true, 7));
            }
        } catch (IOException e) {
            System.out.println("Tiro exception a la loadCategoriesCategoryEdges + " + e);
            e.printStackTrace();
        }
    }



}
