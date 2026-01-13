package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ALEJANDRO.CASTILLO
 */
public class Bean_TR_EXP_CIRCUNST  extends Bean implements SQLData  {
    
    private String SQLTypeName;      
    private String ID_CIRCUNS_MOT_CONF;
    private String ID_ORGANOJ;
    private String ID_EXPEDIENTE;
    private String PERIODO;
    private String ID_UNIQUE;
        
    public String GetID_CIRCUNS_MOT_CONF() {
        return ID_CIRCUNS_MOT_CONF;
    }

    public void SetID_CIRCUNS_MOT_CONF(String ID_CIRCUNS_MOT_CONF) {
        this.ID_CIRCUNS_MOT_CONF = ID_CIRCUNS_MOT_CONF;
    }

    public String GetID_ORGANOJ() {
        return ID_ORGANOJ;
    }

    public void SetID_ORGANOJ(String ID_ORGANOJ) {
        this.ID_ORGANOJ = ID_ORGANOJ;
    }

    public String GetID_EXPEDIENTE() {
        return ID_EXPEDIENTE;
    }

    public void SetID_EXPEDIENTE(String ID_EXPEDIENTE) {
        this.ID_EXPEDIENTE = ID_EXPEDIENTE;
    }

    public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
    }

    public String GetID_UNIQUE() {
        return ID_UNIQUE;
    }

    public void SetID_UNIQUE(String ID_UNIQUE) {
        this.ID_UNIQUE = ID_UNIQUE;
    }
    
     
    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

   
    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
    }
    
    
      @Override
    public Object[] toArray() {
       Object[] array = {
            GetID_CIRCUNS_MOT_CONF(),
            GetID_ORGANOJ(),
            GetID_EXPEDIENTE(),
            GetPERIODO(),
            GetID_UNIQUE()
        };
        return array;
    }
    
     private String ots(Object obj) {
        String val = null;
        if (obj != null) {
            val = obj.toString();
        }
        return val;
    }

    private Integer oti(Object obj) {
        Integer num = null;
        if (obj != null) {
            try {
                num = (Integer) obj;
            } catch (Exception ex) {
                System.out.println("Error en casteo de numero [" + obj + "]");
            }
        }
        return num;
    }



    
}
