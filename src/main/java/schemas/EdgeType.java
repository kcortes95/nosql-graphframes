package schemas;

public enum EdgeType {

	VENUE_TO_SUBCATEGORY(3),
	SUBCATEGORY_TO_CATEGORY(5),
	VENUE_TO_VENUE(7);

	private final int id;

	EdgeType(int id) {
		this.id = id;
	}

	public int getValue() {
		return id;
	}
}
