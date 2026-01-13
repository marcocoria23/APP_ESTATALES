/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Bean_Procedures;

import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Part_Dem_Individual extends Bean implements SQLData {

private String SQLTypeName;    
private String	NOMBRE_ORGANO_JURIS;
private String	CLAVE_ORGANO;
private String	EXPEDIENTE_CLAVE;
private String	ID_DEMANDADO;
private String	DEMANDADO;
private String	DEFENSA_DEM;
private String	TIPO;
private String	RFC_PATRON;
private String	RAZON_SOCIAL_EMPR;
private String	CALLE;
private String	N_EXT;
private String	N_INT;
private String	COLONIA;
private String	CP;
private String	ENTIDAD_NOMBRE_EMPR;
private String	ENTIDAD_CLAVE_EMPR;
private String	MUNICIPIO_NOMBRE_EMPR;
private String	MUNICIPIO_CLAVE_EMPR;
private String	LATITUD1_EMPR;
private String	LONGITUD1_EMPR;
private String	COMENTARIOS;
private String	PERIODO;




    
  
    
    public Part_Dem_Individual() {
    }

   public String GetNOMBRE_ORGANO_JURIS() {
        return NOMBRE_ORGANO_JURIS;
    }

    public void SetNOMBRE_ORGANO_JURIS(String NOMBRE_ORGANO_JURIS) {
        this.NOMBRE_ORGANO_JURIS = NOMBRE_ORGANO_JURIS;
    }
public String GetCLAVE_ORGANO() {
        return CLAVE_ORGANO;
    }

    public void SetCLAVE_ORGANO(String CLAVE_ORGANO) {
        this.CLAVE_ORGANO = CLAVE_ORGANO;
    }
public String GetEXPEDIENTE_CLAVE() {
        return EXPEDIENTE_CLAVE;
    }

    public void SetEXPEDIENTE_CLAVE(String EXPEDIENTE_CLAVE) {
        this.EXPEDIENTE_CLAVE = EXPEDIENTE_CLAVE;
    }
public String GetID_DEMANDADO() {
        return ID_DEMANDADO;
    }

    public void SetID_DEMANDADO(String ID_DEMANDADO) {
        this.ID_DEMANDADO = ID_DEMANDADO;
    }
public String GetDEMANDADO() {
        return DEMANDADO;
    }

    public void SetDEMANDADO(String DEMANDADO) {
        this.DEMANDADO = DEMANDADO;
    }
public String GetDEFENSA_DEM() {
        return DEFENSA_DEM;
    }

    public void SetDEFENSA_DEM(String DEFENSA_DEM) {
        this.DEFENSA_DEM = DEFENSA_DEM;
    }
public String GetTIPO() {
        return TIPO;
    }

    public void SetTIPO(String TIPO) {
        this.TIPO = TIPO;
    }
public String GetRFC_PATRON() {
        return RFC_PATRON;
    }

    public void SetRFC_PATRON(String RFC_PATRON) {
        this.RFC_PATRON = RFC_PATRON;
    }
public String GetRAZON_SOCIAL_EMPR() {
        return RAZON_SOCIAL_EMPR;
    }

    public void SetRAZON_SOCIAL_EMPR(String RAZON_SOCIAL_EMPR) {
        this.RAZON_SOCIAL_EMPR = RAZON_SOCIAL_EMPR;
    }
public String GetCALLE() {
        return CALLE;
    }

    public void SetCALLE(String CALLE) {
        this.CALLE = CALLE;
    }
public String GetN_EXT() {
        return N_EXT;
    }

    public void SetN_EXT(String N_EXT) {
        this.N_EXT = N_EXT;
    }
public String GetN_INT() {
        return N_INT;
    }

    public void SetN_INT(String N_INT) {
        this.N_INT = N_INT;
    }
public String GetCOLONIA() {
        return COLONIA;
    }

    public void SetCOLONIA(String COLONIA) {
        this.COLONIA = COLONIA;
    }
public String GetCP() {
        return CP;
    }

    public void SetCP(String CP) {
        this.CP = CP;
    }
public String GetENTIDAD_NOMBRE_EMPR() {
        return ENTIDAD_NOMBRE_EMPR;
    }

    public void SetENTIDAD_NOMBRE_EMPR(String ENTIDAD_NOMBRE_EMPR) {
        this.ENTIDAD_NOMBRE_EMPR = ENTIDAD_NOMBRE_EMPR;
    }
public String GetENTIDAD_CLAVE_EMPR() {
        return ENTIDAD_CLAVE_EMPR;
    }

    public void SetENTIDAD_CLAVE_EMPR(String ENTIDAD_CLAVE_EMPR) {
        this.ENTIDAD_CLAVE_EMPR = ENTIDAD_CLAVE_EMPR;
    }
public String GetMUNICIPIO_NOMBRE_EMPR() {
        return MUNICIPIO_NOMBRE_EMPR;
    }

    public void SetMUNICIPIO_NOMBRE_EMPR(String MUNICIPIO_NOMBRE_EMPR) {
        this.MUNICIPIO_NOMBRE_EMPR = MUNICIPIO_NOMBRE_EMPR;
    }
public String GetMUNICIPIO_CLAVE_EMPR() {
        return MUNICIPIO_CLAVE_EMPR;
    }

    public void SetMUNICIPIO_CLAVE_EMPR(String MUNICIPIO_CLAVE_EMPR) {
        this.MUNICIPIO_CLAVE_EMPR = MUNICIPIO_CLAVE_EMPR;
    }
public String GetLATITUD1_EMPR() {
        return LATITUD1_EMPR;
    }

    public void SetLATITUD1_EMPR(String LATITUD1_EMPR) {
        this.LATITUD1_EMPR = LATITUD1_EMPR;
    }
public String GetLONGITUD1_EMPR() {
        return LONGITUD1_EMPR;
    }

    public void SetLONGITUD1_EMPR(String LONGITUD1_EMPR) {
        this.LONGITUD1_EMPR = LONGITUD1_EMPR;
    }
public String GetCOMENTARIOS() {
        return COMENTARIOS;
    }

    public void SetCOMENTARIOS(String COMENTARIOS) {
        this.COMENTARIOS = COMENTARIOS;
    }
public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
    }

  
      
      
   
      
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
         //throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object[] toArray() {
        Object[] array = {
            GetNOMBRE_ORGANO_JURIS(),
            GetCLAVE_ORGANO(),
            GetEXPEDIENTE_CLAVE(),
            GetID_DEMANDADO(),
            GetDEMANDADO(),
            GetDEFENSA_DEM(),
            GetTIPO(),
            GetRFC_PATRON(),
            GetRAZON_SOCIAL_EMPR(),
            GetCALLE(),
            GetN_EXT(),
            GetN_INT(),
            GetCOLONIA(),
            GetCP(),
            GetENTIDAD_NOMBRE_EMPR(),
            GetENTIDAD_CLAVE_EMPR(),
            GetMUNICIPIO_NOMBRE_EMPR(),
            GetMUNICIPIO_CLAVE_EMPR(),
            GetLATITUD1_EMPR(),
            GetLONGITUD1_EMPR(),
            GetCOMENTARIOS(),
            GetPERIODO()

     };
     return array;
    }

    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
