package models;

import java.util.Objects;

public class Stop {

    String userid;
    String utctimestamp;
    String tpos;

    public Stop(String userid, String utctimestamp, String tpos) {
        this.userid = userid;
        this.utctimestamp = utctimestamp;
        this.tpos = tpos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stop stop = (Stop) o;
        return Objects.equals(userid, stop.userid) &&
                Objects.equals(tpos, stop.tpos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userid, tpos);
    }

    @Override
    public String toString() {
        return "{" + "userid=" + userid + ", utctimestamp=" + utctimestamp + ", tpos=" + tpos + '}';
    }

}
