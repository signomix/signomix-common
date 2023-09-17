package com.signomix.common.db;

import java.util.List;
import com.signomix.common.Organization;
import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface UserDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void createStructure() throws IotDatabaseException;
    public User getUser(String uid) throws IotDatabaseException ;
    public User getUser(long id) throws IotDatabaseException ;
    public User getUser(String login, String password) throws IotDatabaseException ;
    public List<User> getUsersByRole(String role) throws IotDatabaseException ;
    public void updateUser(User user) throws IotDatabaseException ;
    public List<User> getOrganizationUsers(long organizationId, Integer limit, Integer offset) throws IotDatabaseException ;
    public List<User> getUsers(Integer limit, Integer offset) throws IotDatabaseException ;

    public void removeNotConfirmed(long since);
    public List<User> getAll();
    public void backupDb() throws IotDatabaseException;

    public List<Organization> getOrganizations(Integer limit, Integer offset) throws IotDatabaseException;
    public Organization getOrganization(long id) throws IotDatabaseException;
    public Organization getOrganization(String code) throws IotDatabaseException;
    public void deleteOrganization(long id) throws IotDatabaseException;
    public void addOrganization(Organization organization) throws IotDatabaseException;
    public void updateOrganization(Organization organization) throws IotDatabaseException;
}
