package models;

public class Venue {

	private String id;
	private Double latitude;
	private Double longitude;

	public Venue(String id, Double latitude, Double longitude) {
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Venue venue = (Venue) o;
		return this.id.equals(venue.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	public double distance(Venue other) {
		double R = 6371;
		double radLong1, radLong2, radLat1, radLat2;
		radLong1 = degToRad(longitude);
		radLong2 = degToRad(other.getLongitude());
		radLat1 = degToRad(latitude);
		radLat2 = degToRad(other.getLatitude());
		double delta = radLat1 - radLat2;
		double lambda = radLong1 - radLong2;
		double dist = Math.sin(delta/2) * Math.sin(delta/2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.sin(lambda/2) * Math.sin(lambda/2);
		dist = 2 * Math.atan2(Math.sqrt(dist), Math.sqrt(1-dist));
		return R * dist;
	}

	private double degToRad(double degree) {
		return degree * Math.PI/180;
	}

	public String getId() {
		return id;
	}

	public Double getLatitude() {
		return latitude;
	}

	public Double getLongitude() {
		return longitude;
	}
}
