package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.signomix.common.billing.Order;
import com.signomix.common.db.BillingDaoIface;

import io.agroal.api.AgroalDataSource;

public class BillingDao implements BillingDaoIface {

    @Inject
    Logger logger;

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createStructure() {
        createOrderTable();
    }

    @Override
    public int getOrderCount(int month, int year) {
        String query = "SELECT COUNT(*) FROM orders WHERE month = ? AND year = ?";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, month);
            pstmt.setInt(2, year);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            }
        } catch (Exception e) {
            logger.error("Error getting order count: " + e.getMessage());
        }
        return count;
    }

    @Override
    public Order createOrder(Order order) {
        int actualCount = getOrderCount(getMonthNumber(order.createdAt), getYearNumber(order.createdAt));
        String query = "INSERT INTO orders (id, month, year, created_at, yearly, account_type, user_number) VALUES (?, ?, ?, ?, ?, ?, ?)";
        boolean saved = false;

        while (!saved) {
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                int month = getMonthNumber(order.createdAt);
                int year = getYearNumber(order.createdAt);
                order.id = buildId(actualCount, month, year);
                pstmt.setString(1, order.id);
                pstmt.setInt(2, getMonthNumber(order.createdAt));
                pstmt.setInt(3, getYearNumber(order.createdAt));
                pstmt.setTimestamp(4, order.createdAt);
                pstmt.setBoolean(5, order.yearly);
                pstmt.setInt(6, order.accountType);
                pstmt.setLong(7, order.userNumber);
                pstmt.execute();
            } catch (SQLException e) {
                logger.error("Error saving order: " + e.getMessage());
                // in case of duplicate key, try again
                if (e.getSQLState().equals("23505")) {
                    actualCount = getOrderCount(getMonthNumber(order.createdAt), getYearNumber(order.createdAt));
                } else {
                    order.id = null;
                    break;
                }
            } catch (Exception e) {
                logger.error("Error saving order: " + e.getMessage());
                return order;
            }
            saved = true;
        }
        return order;
    }

    private void createOrderTable(){
        String query = "CREATE TABLE IF NOT EXISTS orders ("
                + "id VARCHAR(255) PRIMARY KEY,"
                + "month INT NOT NULL,"
                + "year INT NOT NULL"
                + "created_at TIMESTAMP NOT NULL,"
                + "yearly BOOLEAN NOT NULL,"
                + "account_type INT NOT NULL,"
                + "user_number BIGINT NOT NULL"
                + ")";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error creating order table: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating order table: " + e.getMessage());
        }
    }

    private String buildId(int lastOrder, int month, int year) {
        String id = lastOrder + "/" + month + "/" + year;
        return id;
    }

    private int getMonthNumber(Timestamp createdAt) {
        LocalDateTime localDateTime = createdAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.getMonthValue();
    }

    private int getYearNumber(Timestamp createdAt) {
        LocalDateTime localDateTime = createdAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.getYear();
    }

}
