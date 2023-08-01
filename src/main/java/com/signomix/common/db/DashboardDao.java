package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;
import com.signomix.common.gui.Dashboard;
import com.signomix.common.gui.DashboardTemplate;

import io.agroal.api.AgroalDataSource;

public class DashboardDao implements DashboardIface {

    Logger logger = Logger.getLogger(DashboardDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "CALL CSVWRITE('backup/dashboards.csv', 'SELECT * FROM dashboards');";
        String query2 = "CALL CSVWRITE('backup/dashboardtemplates.csv', 'SELECT * FROM dashboardtemplates');";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query + query2);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS dashboards ("
                + "id VARCHAR PRIMARY KEY,"
                + "name VARCHAR,"
                + "userid VARCHAR,"
                + "title VARCHAR,"
                + "team VARCHAR,"
                + "widgets VARCHAR,"
                + "token VARCHAR,"
                + "shared BOOLEAN,"
                + "administrators VARCHAR,"
                + "items VARCHAR);"
                + "CREATE TABLE IF NOT EXISTS dashboardtemplates ("
                + "id VARCHAR PRIMARY KEY,"
                + "title VARCHAR,"
                + "widgets VARCHAR,"
                + "items VARCHAR);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void addDashboard(Dashboard dashboard) throws IotDatabaseException {
        String query = "INSERT INTO dashboards (id,name,userid,title,team,widgets,token,shared,administrators,items) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboard.getId());
            pstmt.setString(2, dashboard.getName());
            pstmt.setString(3, dashboard.getUserID());
            pstmt.setString(4, dashboard.getTitle());
            pstmt.setString(5, dashboard.getTeam());
            pstmt.setString(6, dashboard.getWidgetsAsJson());
            pstmt.setString(7, dashboard.getSharedToken());
            pstmt.setBoolean(8, dashboard.isShared());
            pstmt.setString(9, dashboard.getAdministrators());
            pstmt.setString(10, dashboard.getItemsAsJson());
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void removeDashboard(String dashboardId) throws IotDatabaseException {
        String query = "DELETE FROM dashboards WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardId);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public Dashboard getDashboard(String dashboardId) throws IotDatabaseException {
        String query = "SELECT * FROM dashboards WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardId);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    Dashboard dashboard = new Dashboard();
                    dashboard.setId(rs.getString("id"));
                    dashboard.setName(rs.getString("name"));
                    dashboard.setUserID(rs.getString("userid"));
                    dashboard.setTitle(rs.getString("title"));
                    dashboard.setTeam(rs.getString("team"));
                    dashboard.setWidgetsFromJson(rs.getString("widgets"));
                    dashboard.setSharedToken(rs.getString("token"));
                    dashboard.setShared(rs.getBoolean("shared"));
                    dashboard.setAdministrators(rs.getString("administrators"));
                    dashboard.setItemsFromJson(rs.getString("items"));
                    return dashboard;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public void updateDashboard(Dashboard dashboard) throws IotDatabaseException {
        String query = "UPDATE dashboards SET name=?,userid=?,title=?,team=?,widgets=?,token=?,shared=?,administrators=?,items=? WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboard.getName());
            pstmt.setString(2, dashboard.getUserID());
            pstmt.setString(3, dashboard.getTitle());
            pstmt.setString(4, dashboard.getTeam());
            pstmt.setString(5, dashboard.getWidgetsAsJson());
            pstmt.setString(6, dashboard.getSharedToken());
            pstmt.setBoolean(7, dashboard.isShared());
            pstmt.setString(8, dashboard.getAdministrators());
            pstmt.setString(9, dashboard.getItemsAsJson());
            pstmt.setString(10, dashboard.getId());
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public DashboardTemplate getDashboardTemplate(String dashboardTemplateId) throws IotDatabaseException {
        String query = "SELECT * FROM dashboardtemplates WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardTemplateId);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    DashboardTemplate dashboardTemplate = new DashboardTemplate();
                    dashboardTemplate.setId(rs.getString("id"));
                    dashboardTemplate.setTitle(rs.getString("title"));
                    dashboardTemplate.setWidgetsFromJson(rs.getString("widgets"));
                    return dashboardTemplate;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public void saveAsTemplate(Dashboard dashboard) throws IotDatabaseException {
        String query = "INSERT INTO dashboardtemplates (id,title,widgets,items) VALUES (?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboard.getId());
            pstmt.setString(2, dashboard.getTitle());
            pstmt.setString(3, dashboard.getWidgetsAsJson());
            pstmt.setString(4, dashboard.getItemsAsJson());
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void removeTemplate(String dashboardTemplateId) throws IotDatabaseException {
        String query = "DELETE FROM dashboardtemplates WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardTemplateId);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public List<Dashboard> getUserDashboards(String userId, boolean withShared, boolean adminRole, Integer limit,
            Integer offset) throws IotDatabaseException {
        String query = "SELECT * FROM dashboards ";
        if (adminRole) {
            // do nothing
        } else if (withShared) {
            query = query + "WHERE userid=? OR team LIKE ? OR administrators LIKE ?";
        } else {
            query = query + "WHERE userid=?";
        }
        query = query + " ORDER BY name LIMIT ? OFFSET ?";
        String itemsStr;
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            if (adminRole) {
                pstmt.setInt(1, limit);
                pstmt.setInt(2, offset);
            } else {
                if(withShared){
                    pstmt.setString(1, userId);
                    pstmt.setString(2, "%," + userId + ",%");
                    pstmt.setString(3, "%," + userId + ",%");
                    pstmt.setInt(4, limit);
                    pstmt.setInt(5, offset);
                }else{
                    pstmt.setString(1, userId);
                    pstmt.setInt(2, limit);
                    pstmt.setInt(3, offset);
                }
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Dashboard dashboard = new Dashboard();
                    dashboard.setId(rs.getString("id"));
                    dashboard.setName(rs.getString("name"));
                    dashboard.setUserID(rs.getString("userid"));
                    dashboard.setTitle(rs.getString("title"));
                    dashboard.setTeam(rs.getString("team"));
                    dashboard.setWidgetsFromJson(rs.getString("widgets"));
                    dashboard.setSharedToken(rs.getString("token"));
                    dashboard.setShared(rs.getBoolean("shared"));
                    dashboard.setAdministrators(rs.getString("administrators"));
                    itemsStr = rs.getString("items");
                    if (null == itemsStr || itemsStr.isEmpty()) {
                        itemsStr = "[]";
                    }
                    dashboard.setItemsFromJson(itemsStr);
                    dashboards.add(dashboard);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        logger.info("getUserDashboards size: " + dashboards.size());
        return dashboards;
    }

    @Override
    public List<Dashboard> getDashboards(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT * FROM dashboards ORDER BY name LIMIT ? OFFSET ?";
        logger.info("getDashboards: " + query);
        logger.info("getDashboards: " + offset + " " + limit);
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            logger.info("getDashboards datasource url: " + conn.getMetaData().getURL());
            pstmt.setInt(1, limit);
            pstmt.setInt(2, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    logger.info("getDashboards: " + rs.getString("id"));
                    Dashboard dashboard = new Dashboard();
                    dashboard.setId(rs.getString("id"));
                    dashboard.setName(rs.getString("name"));
                    dashboard.setUserID(rs.getString("userid"));
                    dashboard.setTitle(rs.getString("title"));
                    dashboard.setTeam(rs.getString("team"));
                    dashboard.setWidgetsFromJson(rs.getString("widgets"));
                    dashboard.setSharedToken(rs.getString("token"));
                    dashboard.setShared(rs.getBoolean("shared"));
                    dashboard.setAdministrators(rs.getString("administrators"));
                    dashboard.setItemsFromJson(rs.getString("items"));
                    dashboards.add(dashboard);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return dashboards;
    }

}
