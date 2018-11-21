package models;

public class Minmax {

    public long min = 999999999999L;
    public String venueidmin = "";

    public long max = 0;
    public String venueidmax = "";

    public Minmax() {
    }

    public Minmax(long min, long max) {
        this.min = min;
        this.max = max;
    }

    public void set(long num, String venueid){
        if(num <= min){
            min = num;
            venueidmin = venueid;
        }

        if(num > max){
            max = num;
            venueidmax = venueid;
        }

    }

    @Override
    public String toString() {
        return "Minmax{" +
                "min=" + min +
                ", venueidmin='" + venueidmin + '\'' +
                ", max=" + max +
                ", venueidmax='" + venueidmax + '\'' +
                '}';
    }

    public long getMin() {
        return min;
    }

    public String getVenueidmin() {
        return venueidmin;
    }

    public long getMax() {
        return max;
    }

    public String getVenueidmax() {
        return venueidmax;
    }
}
