package com.signomix.common.tsdb;

import java.sql.Connection;
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
import com.signomix.common.db.IotDatabaseException;

import io.agroal.api.AgroalDataSource;

public class BillingDao implements BillingDaoIface {

    private static final Logger logger = Logger.getLogger(BillingDao.class);

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
    public int getOrderCount(int month, int year) throws IotDatabaseException {
        String query = "SELECT COUNT(*) FROM orders WHERE month = ? AND year = ?";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, month);
            pstmt.setInt(2, year);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    count = rs.getInt(1)+1;
                }
            }
        } catch (Exception e) {
            logger.error("Error getting order count: " + e.getMessage());
        }
        return count;
    }

    @Override
    public Order createOrder(Order order) throws IotDatabaseException{
        int actualCount = getOrderCount(getMonthNumber(order.createdAt), getYearNumber(order.createdAt));
        boolean saved = false;
        String query;
        while (!saved) {
            order.id = buildId(actualCount, getMonthNumber(order.createdAt), getYearNumber(order.createdAt));
            query = "INSERT INTO orders (id, month, year, yearly, account_type, target_type, user_number, user_id, "
            + "name, surname, email, address, city, zip, country, tax_no, company_name, currency, price, vat, vat_value, amount, service_name) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, order.id);
                pstmt.setInt(2, getMonthNumber(order.createdAt));
                pstmt.setInt(3, getYearNumber(order.createdAt));
                pstmt.setBoolean(4, order.yearly);
                pstmt.setInt(5, order.accountType);
                pstmt.setInt(6, order.targetType);
                pstmt.setLong(7, order.userNumber);
                pstmt.setString(8, order.uid);
                pstmt.setString(9, order.name);
                pstmt.setString(10, order.surname);
                pstmt.setString(11, order.email);
                pstmt.setString(12, order.address);
                pstmt.setString(13, order.city);
                pstmt.setString(14, order.zip);
                pstmt.setString(15, order.country);
                pstmt.setString(16, order.taxNumber);
                pstmt.setString(17, order.companyName);
                pstmt.setString(18, order.currency);
                pstmt.setDouble(19, order.price);
                pstmt.setString(20, order.tax);
                pstmt.setDouble(21, order.vatValue);
                pstmt.setDouble(22, order.total);
                pstmt.setString(23, order.serviceName);
                pstmt.execute();
                saved = true;
            } catch (SQLException e) {
                logger.error("Error creating order: " + e.getMessage());
                actualCount++;
            } catch (Exception e) {
                logger.error("Error creating order: " + e.getMessage());
                actualCount++;
            }
        }
        return order;
    }

    @Override
    public void updateOrder(Order order) {
        String query = "UPDATE orders SET first_paid_at = ?, next_payment_at = ? WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            if(order.firstPaidAt != null){
                pstmt.setTimestamp(1, order.firstPaidAt);
            } else {
                pstmt.setNull(1, java.sql.Types.TIMESTAMP);
            }
            if(order.nextPaymentAt != null){
                pstmt.setTimestamp(2, order.nextPaymentAt);
            } else {
                pstmt.setNull(2, java.sql.Types.TIMESTAMP);
            }
            pstmt.setString(3, order.id);
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error updating order: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error updating order: " + e.getMessage());
        }
    }

    private void createOrderTable(){
        String query = "CREATE TABLE IF NOT EXISTS orders ("
                + "id VARCHAR(255) PRIMARY KEY,"
                + "month INT NOT NULL,"
                + "year INT NOT NULL,"
                + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "yearly BOOLEAN NOT NULL,"
                + "account_type INT NOT NULL,"
                + "target_type INT,"
                + "user_number BIGINT NOT NULL,"
                + "user_id VARCHAR(255),"
                + "name VARCHAR(255),"
                + "surname VARCHAR(255),"
                + "email VARCHAR(255),"
                + "address VARCHAR(255),"
                + "city VARCHAR(255),"
                + "zip VARCHAR(255),"
                + "country VARCHAR(255),"
                + "tax_no VARCHAR(255),"
                + "company_name VARCHAR(255),"
                + "currency VARCHAR(255),"
                + "first_paid_at TIMESTAMP,"
                + "next_payment_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "service_name VARCHAR(255),"
                + "price DECIMAL(10,2),"
                + "vat VARCHAR(10),"
                + "vat_value DECIMAL(10,2),"
                + "amount DECIMAL(10,2)"
                + ")";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error creating order tabl\n" +  e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating order table: " + e.getMessage());
        }

        //create index for month and year
        query = "CREATE INDEX IF NOT EXISTS idx_month_year ON orders (month, year)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.warn("Error creating index for month and year: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating index for month and year: " + e.getMessage());
        }
        //create index for vat
        query = "CREATE INDEX IF NOT EXISTS idx_vat ON orders (vat)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.warn("Error creating index for vat: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating index for vat: " + e.getMessage());
        }
        //create index for user_id
        query = "CREATE INDEX IF NOT EXISTS idx_user_id ON orders (user_id)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.warn("Error creating index for user_id: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating index for user_id: " + e.getMessage());
        }   
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query=
        "COPY orders TO '/var/lib/postgresql/data/export/orders.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("backupDb", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("backupDb", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

    }

    private String buildId(int lastOrder, int month, int year) {
        String id = "Z/"+lastOrder + "/" + month + "/" + year+"/S";
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

    @Override
    public Order getOrder(String id) throws IotDatabaseException {
        String query = "SELECT * FROM orders WHERE id = ?";
        Order order = new Order();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    order.id = rs.getString("id");
                    order.createdAt = rs.getTimestamp("created_at");
                    order.yearly = rs.getBoolean("yearly");
                    order.accountType = rs.getInt("account_type");
                    order.targetType = rs.getInt("target_type");
                    order.userNumber = rs.getLong("user_number");
                    order.uid = rs.getString("user_id");
                    order.firstPaidAt = rs.getTimestamp("first_paid_at");
                    order.nextPaymentAt = rs.getTimestamp("next_payment_at");
                    order.name = rs.getString("name");
                    order.surname = rs.getString("surname");
                    order.email = rs.getString("email");
                    order.address = rs.getString("address");
                    order.city = rs.getString("city");
                    order.zip = rs.getString("zip");
                    order.country = rs.getString("country");
                    order.taxNumber = rs.getString("tax_no");
                    order.companyName = rs.getString("company_name");
                    order.currency = rs.getString("currency");
                    order.price = rs.getDouble("price");
                    order.tax = rs.getString("vat");
                    order.vatValue = rs.getDouble("vat_value");
                    order.total = rs.getDouble("amount");
                    order.serviceName = rs.getString("service_name");
                }
            }
        } catch (Exception e) {
            logger.error("Error getting order: " + e.getMessage());
        }
        return order;
    }



}
