package models;

public class Utils {

    public static String categoryPath = "/home/kcortesrodrigue/category.csv";
    public static String categoriesPath = "/home/kcortesrodrigue/categories.csv";
    public static String venuesPath = "/home/kcortesrodrigue/venues.csv";
    //public static final String stopPath = "/home/kcortesrodrigue/stops.csv";
    public static String stopPath = "/home/kcortesrodrigue/stops_new.csv";

    public static void setCategoryPath(String categoryPath) {
        Utils.categoryPath = categoryPath;
    }

    public static void setCategoriesPath(String categoriesPath) {
        Utils.categoriesPath = categoriesPath;
    }

    public static void setVenuesPath(String venuesPath) {
        Utils.venuesPath = venuesPath;
    }

    public static void setStopPath(String stopPath) {
        Utils.stopPath = stopPath;
    }
}
