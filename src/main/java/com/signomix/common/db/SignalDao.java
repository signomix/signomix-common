package com.signomix.common.db;

import java.util.List;

import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;

public class SignalDao implements SignalDaoIface {

    @Override
    public void setDatasource(AgroalDataSource ds) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setDatasource'");
    }

    @Override
    public AgroalDataSource getDataSource() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDataSource'");
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createStructure'");
    }

    @Override
    public void saveSignal(Signal signal) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'saveSignal'");
    }

    @Override
    public Signal getSignalById(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSignalById'");
    }

    @Override
    public void updateSignal(Signal signal) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateSignal'");
    }

    @Override
    public void deleteSignal(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteSignal'");
    }

    @Override
    public List<Signal> getUserSignals(String userId, int limit, int offset) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getUserSignals'");
    }

    @Override
    public List<Signal> getOrganizationSignals(long organizationId, int limit, int offset) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganizationSignals'");
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'backupDb'");
    }

    @Override
    public void archiveSignals(long checkpoint) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'archiveSignals'");
    }

    @Override
    public void clearOldSignals(long checkpoint) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearOldSignals'");
    }

    @Override
    public void archiveUserSignals(long checkpoint) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'archiveUserSignals'");
    }

    @Override
    public void clearOldUserSignals(long checkpoint) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearOldUserSignals'");
    }
    
}
