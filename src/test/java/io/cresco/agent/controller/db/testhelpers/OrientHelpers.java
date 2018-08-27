package io.cresco.agent.controller.db.testhelpers;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;
import java.util.logging.Logger;

public class OrientHelpers {
   public static OCommandOutputListener getCommandListener() {
        return new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
                if(iText.matches(".*error.*")) {
                    System.out.print(iText);
                }
            }
        };
    }


    public static ODatabaseDocumentTx getOrientTx(GDBConf gdbConf){
        return new ODatabaseDocumentTx(gdbConf.getRemoteURI()).open(gdbConf.getUsername(),gdbConf.getPassword());
    }

    public static Boolean addDbUser(GDBConf gdbAdminConf, String username, String password, String[] roles) {
        ODatabaseDocumentTx db = getOrientTx(gdbAdminConf);
        Boolean addSuccess = false;
        try {
            OSecurity secMan = db.getMetadata().getSecurity();
            secMan.dropUser(username);
            OUser newUser = secMan.createUser(username,password,roles);
            newUser.setAccountStatus(OSecurityUser.STATUSES.ACTIVE);
            newUser.save();
            addSuccess = true;
        }
        catch(Exception ex){
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        finally{
            db.close();
        }
        return addSuccess;
    }

    public static Boolean importFileToOrientDB(GDBConf gdbConf, String filePath){
        //String dbURI = "remote:"+gdbConf.get("gdb_host")+"/"+gdbConf.get("gdb_dbname");
        //new ODatabaseDocumentTx(dbURI).open((String)gdbConf.get("gdb_username")
                //,(String)gdbConf.get("gdb_password") );
        ODatabaseDocumentTx db = getOrientTx(gdbConf);
        try{
            //OCommandOutputListener importListener = getCommandListener();
            ODatabaseImport imp = new ODatabaseImport(db, filePath, getCommandListener());
            imp.importDatabase();
            imp.close();
        }catch(Exception ex){
            Logger.getLogger("CrescoHelpers").severe("Unhandled exception caught during DB import");
            ex.printStackTrace();
            return false;
        }
        finally {
            db.close();
        }
        return true;
    }

    public static Boolean resetDB (GDBConf adminConf, String dbBackupPath) throws IOException {
        //OConsoleDatabaseApp cons = new OGremlinConsole(null);
        OServerAdmin sa = new OServerAdmin(adminConf.getRemoteURI());
        Boolean success = false;
        ;
        try {
            sa.connect(adminConf.getUsername(),adminConf.getPassword());
            if(sa.existsDatabase()){
                sa.dropDatabase("PLOCAL");
            }
            sa.createDatabase("graph","memory");
            FileInputStream backupFile = new FileInputStream(dbBackupPath);
            try(ODatabaseDocumentTx db = getOrientTx(adminConf)) {
                db.begin();
                db.restore(backupFile, null, null, getCommandListener());
                db.commit();
            }
            catch(Exception ex){
                System.out.println("Error getting orient tx during restore");
                ex.printStackTrace();
            }
        }
        catch(Exception ex){
            System.out.println("Could not reset orient db: "+ex.getMessage());
            ex.printStackTrace();
        }
        finally{
            sa.close();
        }

        return success;
    }

    public static Optional<ODatabaseDocumentTx> getInMemoryTestDB(InputStream dbBackup) throws FileNotFoundException,IOException{
        FileInputStream backupFile;

        ODatabaseDocumentTx newdb=null;
        //try{
            newdb = new ODatabaseDocumentTx("memory:internalDb").create();
            //newdb.restore(backupFile,null,null,getCommandListener());
            ODatabaseImport imp = new ODatabaseImport(newdb, dbBackup, getCommandListener());
            imp.importDatabase();
            imp.close();
        /*}
        catch(FileNotFoundException ex){
            System.out.println("Can't find backup file to create new db");
            ex.printStackTrace();
        }
        catch(IOException ex){
            System.out.println("Can't open internal DB instance for some reason");
            ex.printStackTrace();
        }*/
        return Optional.ofNullable(newdb);
    }

}
