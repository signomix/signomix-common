package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public class UserDao implements UserDaoIface {
    private static final Logger LOG = Logger.getLogger(UserDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource=ds;
    }

    @Override
    public String getUser(String uid) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeNotConfirmed(long days) {
        String query = "DELETE FROM users WHERE confirmed=false AND "+days+">TIMESTAMPDIFF(DAY, CURRENT_TIMESTAMP, created) ";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) { 
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            //throw new KeyValueDBException(e.getErrorCode(), e.getMessage());
        }
    }

    @Override
    public List<User> getAll(){
        return new ArrayList<>();
    }

    User buildUser(ResultSet rs) throws SQLException {
        //uid,type,email,name,surname,role,secret,password,generalchannel,infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,number,services,phoneprefix,credits,autologin
        User user = new User();
        user.uid=rs.getString(1);
        user.type=rs.getInt(2);
        user.email=rs.getString(3);
        user.name=rs.getString(4);
        user.surname=rs.getString(5);
        user.role=rs.getString(6);
        user.confirmString=rs.getString(7);
        user.password=rs.getString(8);
        user.generalNotificationChannel=rs.getString(9);
        user.infoNotificationChannel=rs.getString(10);
        user.warningNotificationChannel=rs.getString(11);
        user.alertNotificationChannel=rs.getString(12);
        user.confirmed=rs.getBoolean(13);
        user.unregisterRequested=rs.getBoolean(14);
        user.authStatus=rs.getInt(15);
        user.createdAt=rs.getTimestamp(16).getTime();
        user.number=rs.getLong(17);
        user.services=rs.getInt(18);
        user.phonePrefix=rs.getString(19);
        user.credits=rs.getLong(20);
        user.autologin=rs.getBoolean(21);
        user.preferredLanguage=rs.getString(22);
        return user;
    }
    
}
