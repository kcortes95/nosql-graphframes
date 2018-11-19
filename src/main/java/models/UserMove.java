package models;

import java.sql.Date;
import java.util.Objects;

public class UserMove {

    String userid;
    Date utctimestamp;

    public UserMove() {
    }

    public UserMove(String userid, Date utctimestamp) {
        this.userid = userid;
        this.utctimestamp = utctimestamp;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public Date getUtctimestamp() {
        return utctimestamp;
    }

    public void setUtctimestamp(Date utctimestamp) {
        this.utctimestamp = utctimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserMove userMove = (UserMove) o;
        return Objects.equals(userid, userMove.userid) &&
                Objects.equals(utctimestamp, userMove.utctimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userid, utctimestamp);
    }

    @Override
    public String toString() {
        return "UserMove{" +
                "userid='" + userid + '\'' +
                ", utctimestamp=" + utctimestamp +
                '}';
    }
}
