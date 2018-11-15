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

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getUtctimestamp() {
        return utctimestamp;
    }

    public void setUtctimestamp(String utctimestamp) {
        this.utctimestamp = utctimestamp;
    }

    public String getTpos() {
        return tpos;
    }

    public void setTpos(String tpos) {
        this.tpos = tpos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stop stop = (Stop) o;
        return Objects.equals(userid, stop.userid) &&
                Objects.equals(utctimestamp, stop.utctimestamp) &&
                Objects.equals(tpos, stop.tpos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userid, utctimestamp, tpos);
    }

    @Override
    public String toString() {
        return "{" + "userid=" + userid + ", utctimestamp=" + utctimestamp + ", tpos=" + tpos + '}';
    }

}
