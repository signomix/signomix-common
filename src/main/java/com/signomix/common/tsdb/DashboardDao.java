package com.signomix.common.tsdb;

import com.signomix.common.db.DashboardIface;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.gui.Dashboard;
import com.signomix.common.gui.DashboardTemplate;
import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

public class DashboardDao implements DashboardIface {

    private static final Logger logger = Logger.getLogger(DashboardDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        // TODO: implement
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        /*
         * String query = "CREATE TABLE IF NOT EXISTS dashboards ("
         * + "id VARCHAR PRIMARY KEY,"
         * + "name VARCHAR,"
         * + "userid VARCHAR,"
         * + "title VARCHAR,"
         * + "team VARCHAR,"
         * + "widgets VARCHAR,"
         * + "token VARCHAR,"
         * + "shared BOOLEAN,"
         * + "administrators VARCHAR,"
         * + "items VARCHAR,"
         * + "organization BIGINT);"
         * + "CREATE TABLE IF NOT EXISTS dashboardtemplates ("
         * + "id VARCHAR PRIMARY KEY,"
         * + "title VARCHAR,"
         * + "widgets VARCHAR,"
         * + "items VARCHAR,"
         * + "organization BIGINT);";
         * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
         * conn.prepareStatement(query);) {
         * pstmt.execute();
         * } catch (SQLException e) {
         * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
         * e.getMessage(), e);
         * } catch (Exception e) {
         * throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
         * }
         * 
         * // "favourites" table is crrated in IotDatabaseDao class
         */
    }

    @Override
    public void addDashboard(Dashboard dashboard) throws IotDatabaseException {
        String query = "INSERT INTO dashboards (id,name,userid,title,team,widgets,token,shared,administrators,items,organization) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
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
            pstmt.setLong(11, dashboard.getOrganizationId());
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
        throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "Temporarily not allowed");
        /* try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardId);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        } */
    }

    @Override
    public Dashboard getDashboard(String dashboardId) throws IotDatabaseException {
        Dashboard dashboard=null;
        String query = "SELECT * FROM dashboards WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardId);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    dashboard = new Dashboard();
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
                    dashboard.setOrganizationId(rs.getLong("organization"));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return dashboard;
    }

    @Override
    public void updateDashboard(Dashboard dashboard) throws IotDatabaseException {
        String query = "UPDATE dashboards SET "
                + "name=?,userid=?,title=?,team=?,widgets=?,token=?,shared=?,administrators=?,items=?,organization=? WHERE id=?";
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
            pstmt.setLong(10, dashboard.getOrganizationId());
            pstmt.setString(11, dashboard.getId());
            int count = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public DashboardTemplate getDashboardTemplate(String dashboardTemplateId) throws IotDatabaseException {
        DashboardTemplate dashboardTemplate=null;
        String query = "SELECT * FROM dashboardtemplates WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardTemplateId);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    dashboardTemplate = new DashboardTemplate();
                    dashboardTemplate.setId(rs.getString("id"));
                    dashboardTemplate.setTitle(rs.getString("title"));
                    dashboardTemplate.setWidgetsFromJson(rs.getString("widgets"));
                    
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return dashboardTemplate;
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
            Integer offset, String searchString) throws IotDatabaseException {

        String searchCondition = "";
        String[] searchParts;
        if (null == searchString || searchString.isEmpty()) {
            searchParts = new String[0];
        } else {
            searchParts = searchString.split(":");
            if (searchParts.length == 2) {
                if (searchParts[0].equals("id")) {
                    searchCondition = " LOWER(d.id) LIKE LOWER(?) ";
                } else if (searchParts[0].equals("title")) {
                    searchCondition = " LOWER(d.title) LIKE LOWER(?) ";
                }
            }
        }
        String query = "SELECT "
                + "d.id,d.name,d.userid,d.title,d.team,d.widgets,d.token,d.shared,d.administrators,d.items,d.organization,"
                + "(SELECT COUNT(*) FROM favourites as f where f.userid=? and f.id=d.id and f.is_device=false) AS favourite"
                + " FROM dashboards AS d ";
        if (adminRole) {
            // do nothing
        } else if (withShared) {
            query = query + "WHERE (d.userid=? OR d.team LIKE ? OR d.administrators LIKE ?) ";
        } else {
            query = query + "WHERE d.userid=?";
        }
        if(!searchCondition.isEmpty()){
            if(adminRole){
                query = query + " WHERE ";
            }else{
                query = query + " AND ";
            }
            query = query + searchCondition;
        }
        query = query + " ORDER BY d.title LIMIT ? OFFSET ?";
        //logger.info("getUserDashboards: " + query);
        String itemsStr;
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userId);
            int idxLimit = 2;
            if (adminRole) {
                //pstmt.setInt(2, limit);
                //pstmt.setInt(3, offset);
            } else {
                if (withShared) {
                    pstmt.setString(2, userId);
                    pstmt.setString(3, "%," + userId + ",%");
                    pstmt.setString(4, "%," + userId + ",%");
                    idxLimit = 5;
                } else {
                    pstmt.setString(2, userId);
                    idxLimit = 3;
                }
            }
            if(!searchCondition.isEmpty()){
                pstmt.setString(idxLimit, "%" + searchParts[1] + "%");
                idxLimit++;
            }
            pstmt.setInt(idxLimit, limit);
            pstmt.setInt(idxLimit+1, offset);
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
                    dashboard.setOrganizationId(rs.getLong("organization"));
                    dashboard.setFavourite(rs.getInt("favourite") > 0);
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
        logger.debug("getUserDashboards size: " + dashboards.size());
        return dashboards;
    }

    @Override
    public List<Dashboard> getDashboards(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT * FROM dashboards ORDER BY name LIMIT ? OFFSET ?";
        // logger.info("getDashboards: " + query);
        // logger.info("getDashboards: " + offset + " " + limit);
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            // logger.info("getDashboards datasource url: " + conn.getMetaData().getURL());
            pstmt.setInt(1, limit);
            pstmt.setInt(2, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    logger.debug("getDashboards: " + rs.getString("id"));
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
                    dashboard.setOrganizationId(rs.getLong("organization"));
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

    @Override
    public List<Dashboard> getOrganizationDashboards(long organizationId, Integer limit, Integer offset)
            throws IotDatabaseException {
        String query = "SELECT * FROM dashboards WHERE organization= ? ORDER BY name LIMIT ? OFFSET ?";
        logger.debug("getOrganizationDashboards: " + organizationId);
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    logger.debug("getDashboards: " + rs.getString("id"));
                    Dashboard dashboard = new Dashboard();
                    dashboard.setId(rs.getString("id"));
                    dashboard.setName(rs.getString("name"));
                    dashboard.setUserID(rs.getString("userid"));
                    dashboard.setTitle(rs.getString("title"));
                    dashboard.setTeam(rs.getString("team"));
                    dashboard.setSharedToken(rs.getString("token"));
                    dashboard.setShared(rs.getBoolean("shared"));
                    dashboard.setAdministrators(rs.getString("administrators"));
                    dashboard.setOrganizationId(rs.getLong("organization"));
                    dashboard.setItemsFromJson(rs.getString("items"));
                    dashboard.setWidgetsFromJson(rs.getString("widgets"));
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

    @Override
    public void addFavouriteDashboard(String userID, String dashboardID) throws IotDatabaseException {
        String query = "INSERT INTO favourites (userid,id,is_device) VALUES (?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setString(2, dashboardID);
            pstmt.setBoolean(3, false);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void removeFavouriteDashboard(String userID, String dashboardID) throws IotDatabaseException {
        String query = "DELETE FROM favourites WHERE userid=? AND id=? AND is_device=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setString(2, dashboardID);
            pstmt.setBoolean(3, false);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<Dashboard> getFavouriteDashboards(String userID) throws IotDatabaseException {
        String query = "SELECT * FROM dashboards WHERE id IN (SELECT id FROM favourites WHERE userid=? AND is_device=?)";
        List<Dashboard> dashboards = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setBoolean(2, false);
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
                    dashboard.setItemsFromJson(rs.getString("items"));
                    dashboard.setOrganizationId(rs.getLong("organization"));
                    dashboards.add(dashboard);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return dashboards;
    }

    @Override
    public List<DashboardTemplate> getDashboardTemplates(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT * FROM dashboardtemplates ORDER BY title LIMIT ? OFFSET ?";
        List<DashboardTemplate> dashboardTemplates = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, limit);
            pstmt.setInt(2, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    DashboardTemplate dashboardTemplate = new DashboardTemplate();
                    dashboardTemplate.setId(rs.getString("id"));
                    dashboardTemplate.setTitle(rs.getString("title"));
                    dashboardTemplate.setWidgetsFromJson(rs.getString("widgets"));
                    dashboardTemplate.setItemsFromJson(rs.getString("items"));
                    dashboardTemplate.setOrganizationId(rs.getLong("organization"));
                    dashboardTemplates.add(dashboardTemplate);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return dashboardTemplates;
    }

    @Override
    public void addDashboardTemplate(DashboardTemplate dashboardTemplate) throws IotDatabaseException {
        String query = "INSERT INTO dashboardtemplates (id,title,widgets,items,organization) VALUES (?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardTemplate.getId());
            pstmt.setString(2, dashboardTemplate.getTitle());
            pstmt.setString(3, dashboardTemplate.getWidgetsAsJson());
            pstmt.setString(4, dashboardTemplate.getItemsAsJson());
            pstmt.setLong(5, dashboardTemplate.getOrganizationId());
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

}
