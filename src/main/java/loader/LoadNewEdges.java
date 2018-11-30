package loader;

import cellindexmethod.Cell;
import cellindexmethod.Grid;
import models.Utils;
import models.Venue;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import schemas.EdgeType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;

public class LoadNewEdges {

	public static ArrayList<Row> LoadEdges(double maxDistance) {
		ArrayList<Row> edges = new ArrayList<>();

		//loadVenuesCategoriesEdges(LoadNewVertices.getVenues(), LoadNewVertices.getCategories(), edges, Utils.venuesPath);
		//loadCategoriesCategoryEdges(LoadNewVertices.getCategories(), LoadNewVertices.getCategory(), edges, Utils.categoriesPath);
		loadVenueVenueEdges(LoadNewVertices.getVenues(), LoadNewVertices.getGrid(), edges, maxDistance);

		return edges;
	}

	private static void loadVenuesCategoriesEdges(Map<String, Long> venues, Map<String, Long> categories, ArrayList<Row> edges, String path) {

		try (Stream<String> stream = Files.lines(Paths.get(path))) {
			Object[] arr = stream.toArray();
			for (long i = 1; i < arr.length; i++) {
				String data = (String) arr[(int) i];
				String datas[] = data.split(",");

				String venueid = datas[0].replace("\"", "");
				String category = datas[1].replace("\"", "");

				Long from = venues.get(venueid);
				Long to = categories.get(category);

				edges.add(RowFactory.create(from, to, false, true, false, EdgeType.VENUE_TO_SUBCATEGORY.getValue()));
			}
		} catch (IOException e) {
			System.out.println("Tiro exception a la loadVenuesCategoriesEdges + " + e);
			e.printStackTrace();
		}
	}

	private static void loadCategoriesCategoryEdges(Map<String, Long> categories, Map<String, Long> category,
													ArrayList<Row> edges, String path) {

		try (Stream<String> stream = Files.lines(Paths.get(path))) {
			Object[] arr = stream.toArray();
			for (int i = 1; i < arr.length; i++) {
				String data = (String) arr[i];
				String datas[] = data.split(",");

				String cats = datas[0].replace("\"", "");
				String cat = datas[1].replace("\"", "");

				Long from = categories.get(cats);
				Long to = category.get(cat);

				edges.add(RowFactory.create(from, to, false, false, true, EdgeType.SUBCATEGORY_TO_CATEGORY.getValue()));
			}
		} catch (IOException e) {
			System.out.println("Tiro exception a la loadCategoriesCategoryEdges + " + e);
			e.printStackTrace();
		}
	}

	private static void loadVenueVenueEdges(Map<String, Long> venues, Grid grid, ArrayList<Row> edges, double maxDistance) {
		for (int i = 0; i < grid.getM(); i++) {
			for (int j = 0; j < grid.getM(); j++) {
				for (Venue v : grid.getCell(i, j).getVenues()) {
					for (Venue v2 : grid.getCell(i, j).getVenues()) {
						if (!v.equals(v2) && v.distance(v2) <= maxDistance) {
							edges.add(RowFactory.create(venues.get(v.getId()), venues.get(v2.getId()), true, false, false, EdgeType.VENUE_TO_VENUE.getValue()));
						}
					}
					for (Cell c : grid.getCell(i, j).getNeighbours()) {
						for (Venue v2 : c.getVenues()) {
							if (!v.equals(v2) && v.distance(v2) <= maxDistance) {
								edges.add(RowFactory.create(venues.get(v.getId()), venues.get(v2.getId()), true, false, false, EdgeType.VENUE_TO_VENUE.getValue()));
								edges.add(RowFactory.create(venues.get(v2.getId()), venues.get(v.getId()), true, false, false, EdgeType.VENUE_TO_VENUE.getValue()));
							}
						}
					}
				}
			}
		}
	}
}
