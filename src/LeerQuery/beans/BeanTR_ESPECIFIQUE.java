/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package LeerQuery.beans;


import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author JOSE.CABRERA
 */

public class BeanTR_ESPECIFIQUE extends Bean implements SQLData  {
    
        private String SQLTypeName;
        private String ID_REGISTRO;
        private String NEMONICO_CATALOGO;
        private String ESPECIFIQUE;
        private String ID_OTRO_ESPECIFIQUE;
        private String CLAVE_EXPEDIENTE;
        private String PROCEDIMIENTO;
        private String ORGANO;
        private String ID_ACT_DEM_AUD;
        private String PERIODO;
        
        
     public String GetID_REGISTRO() {
        return ID_REGISTRO;
    }

    public void SetID_REGISTRO(String ID_REGISTRO) {
        this.ID_REGISTRO = ID_REGISTRO;
    }
    
   public String GetNEMONICO_CATALOGO() {
        return NEMONICO_CATALOGO;
    }

    public void SetNEMONICO_CATALOGO(String NEMONICO_CATALOGO) {
        this.NEMONICO_CATALOGO = NEMONICO_CATALOGO;
    }
    
   public String GetESPECIFIQUE() {
        return ESPECIFIQUE;
    }

    public void SetESPECIFIQUE(String ESPECIFIQUE) {
        this.ESPECIFIQUE = ESPECIFIQUE;
    }
    
   public String GetID_OTRO_ESPECIFIQUE() {
        return ID_OTRO_ESPECIFIQUE;
    }

    public void SetID_OTRO_ESPECIFIQUE(String ID_OTRO_ESPECIFIQUE) {
        this.ID_OTRO_ESPECIFIQUE = ID_OTRO_ESPECIFIQUE;
    }
    
   public String GetCLAVE_EXPEDIENTE() {
        return CLAVE_EXPEDIENTE;
    }

    public void SetCLAVE_EXPEDIENTE(String CLAVE_EXPEDIENTE) {
        this.CLAVE_EXPEDIENTE = CLAVE_EXPEDIENTE;
    }

   public String GetPROCEDIMIENTO() {
        return PROCEDIMIENTO;
    }

    public void SetPROCEDIMIENTO(String PROCEDIMIENTO) {
        this.PROCEDIMIENTO = PROCEDIMIENTO;
    }
    
   public String GetORGANO() {
        return ORGANO;
    }

    public void SetORGANO(String ORGANO) {
        this.ORGANO = ORGANO;
    }
    
   public String GetID_ACT_DEM_AUD() {
        return ID_ACT_DEM_AUD;
    }

    public void SetID_ACT_DEM_AUD(String ID_ACT_DEM_AUD) {
        this.ID_ACT_DEM_AUD = ID_ACT_DEM_AUD;
    }
    
   public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
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
            GetID_REGISTRO(),
            GetNEMONICO_CATALOGO(),
            GetESPECIFIQUE(),
            GetID_OTRO_ESPECIFIQUE(),
            GetCLAVE_EXPEDIENTE(),
            GetPROCEDIMIENTO(),
            GetORGANO(),
            GetID_ACT_DEM_AUD(),
            GetPERIODO()
                

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
