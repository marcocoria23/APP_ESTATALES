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
public class Part_Dem_Colect_Econom  extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String ID_DEMANDADO;
    private String DEMANDADO;
    private String DEFENSA_DEM;
    private String NOMBRE_SINDICATO_DEM;
    private String REG_ASOC_SINDICAL_DEM;
    private String TIPO_SINDICATO_DEM;
    private String OTRO_ESP_SINDICATO_DEM;
    private String ORG_OBRERA_DEM;
    private String NOMBRE_ORG_OBRERA_DEM;
    private String OTRO_ESP_OBRERA_DEM;
    private String CANT_TRABAJA_INV_DEM;
    private String HOMBRES_DEM;
    private String MUJERES_DEM;
    private String NO_IDENTIFICADO_DEM;
    private String TIPO_DEM_PAT;
    private String RFC_PATRON_DEM;
    private String RAZON_SOCIAL_EMPR_DEM;
    private String CALLE;
    private String N_EXT;
    private String N_INT;
    private String COLONIA;
    private String CP;
    private String ENTIDAD_NOMBRE_EMPR;
    private String ENTIDAD_CLAVE_EMPR;
    private String MUNICIPIO_NOMBRE_EMPR;
    private String MUNICIPIO_CLAVE_EMPR;
    private String LATITUD_EMPR1;
    private String LONGITUD_EMPR1;
    private String COMENTARIOS;
    private String PERIODO;



    
    
    public Part_Dem_Colect_Econom(){   
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
public String GetNOMBRE_SINDICATO_DEM() {
        return NOMBRE_SINDICATO_DEM;
    }

    public void SetNOMBRE_SINDICATO_DEM(String NOMBRE_SINDICATO_DEM) {
        this.NOMBRE_SINDICATO_DEM = NOMBRE_SINDICATO_DEM;
    }
public String GetREG_ASOC_SINDICAL_DEM() {
        return REG_ASOC_SINDICAL_DEM;
    }

    public void SetREG_ASOC_SINDICAL_DEM(String REG_ASOC_SINDICAL_DEM) {
        this.REG_ASOC_SINDICAL_DEM = REG_ASOC_SINDICAL_DEM;
    }
public String GetTIPO_SINDICATO_DEM() {
        return TIPO_SINDICATO_DEM;
    }

    public void SetTIPO_SINDICATO_DEM(String TIPO_SINDICATO_DEM) {
        this.TIPO_SINDICATO_DEM = TIPO_SINDICATO_DEM;
    }
public String GetOTRO_ESP_SINDICATO_DEM() {
        return OTRO_ESP_SINDICATO_DEM;
    }

    public void SetOTRO_ESP_SINDICATO_DEM(String OTRO_ESP_SINDICATO_DEM) {
        this.OTRO_ESP_SINDICATO_DEM = OTRO_ESP_SINDICATO_DEM;
    }
public String GetORG_OBRERA_DEM() {
        return ORG_OBRERA_DEM;
    }

    public void SetORG_OBRERA_DEM(String ORG_OBRERA_DEM) {
        this.ORG_OBRERA_DEM = ORG_OBRERA_DEM;
    }
public String GetNOMBRE_ORG_OBRERA_DEM() {
        return NOMBRE_ORG_OBRERA_DEM;
    }

    public void SetNOMBRE_ORG_OBRERA_DEM(String NOMBRE_ORG_OBRERA_DEM) {
        this.NOMBRE_ORG_OBRERA_DEM = NOMBRE_ORG_OBRERA_DEM;
    }
public String GetOTRO_ESP_OBRERA_DEM() {
        return OTRO_ESP_OBRERA_DEM;
    }

    public void SetOTRO_ESP_OBRERA_DEM(String OTRO_ESP_OBRERA_DEM) {
        this.OTRO_ESP_OBRERA_DEM = OTRO_ESP_OBRERA_DEM;
    }
public String GetCANT_TRABAJA_INV_DEM() {
        return CANT_TRABAJA_INV_DEM;
    }

    public void SetCANT_TRABAJA_INV_DEM(String CANT_TRABAJA_INV_DEM) {
        this.CANT_TRABAJA_INV_DEM = CANT_TRABAJA_INV_DEM;
    }
public String GetHOMBRES_DEM() {
        return HOMBRES_DEM;
    }

    public void SetHOMBRES_DEM(String HOMBRES_DEM) {
        this.HOMBRES_DEM = HOMBRES_DEM;
    }
public String GetMUJERES_DEM() {
        return MUJERES_DEM;
    }

    public void SetMUJERES_DEM(String MUJERES_DEM) {
        this.MUJERES_DEM = MUJERES_DEM;
    }
public String GetNO_IDENTIFICADO_DEM() {
        return NO_IDENTIFICADO_DEM;
    }

    public void SetNO_IDENTIFICADO_DEM(String NO_IDENTIFICADO_DEM) {
        this.NO_IDENTIFICADO_DEM = NO_IDENTIFICADO_DEM;
    }
public String GetTIPO_DEM_PAT() {
        return TIPO_DEM_PAT;
    }

    public void SetTIPO_DEM_PAT(String TIPO_DEM_PAT) {
        this.TIPO_DEM_PAT = TIPO_DEM_PAT;
    }
public String GetRFC_PATRON_DEM() {
        return RFC_PATRON_DEM;
    }

    public void SetRFC_PATRON_DEM(String RFC_PATRON_DEM) {
        this.RFC_PATRON_DEM = RFC_PATRON_DEM;
    }
public String GetRAZON_SOCIAL_EMPR_DEM() {
        return RAZON_SOCIAL_EMPR_DEM;
    }

    public void SetRAZON_SOCIAL_EMPR_DEM(String RAZON_SOCIAL_EMPR_DEM) {
        this.RAZON_SOCIAL_EMPR_DEM = RAZON_SOCIAL_EMPR_DEM;
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
public String GetLATITUD_EMPR1() {
        return LATITUD_EMPR1;
    }

    public void SetLATITUD_EMPR1(String LATITUD_EMPR1) {
        this.LATITUD_EMPR1 = LATITUD_EMPR1;
    }
public String GetLONGITUD_EMPR1() {
        return LONGITUD_EMPR1;
    }

    public void SetLONGITUD_EMPR1(String LONGITUD_EMPR1) {
        this.LONGITUD_EMPR1 = LONGITUD_EMPR1;
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
            GetNOMBRE_SINDICATO_DEM(),
            GetREG_ASOC_SINDICAL_DEM(),
            GetTIPO_SINDICATO_DEM(),
            GetOTRO_ESP_SINDICATO_DEM(),
            GetORG_OBRERA_DEM(),
            GetNOMBRE_ORG_OBRERA_DEM(),
            GetOTRO_ESP_OBRERA_DEM(),
            GetCANT_TRABAJA_INV_DEM(),
            GetHOMBRES_DEM(),
            GetMUJERES_DEM(),
            GetNO_IDENTIFICADO_DEM(),
            GetTIPO_DEM_PAT(),
            GetRFC_PATRON_DEM(),
            GetRAZON_SOCIAL_EMPR_DEM(),
            GetCALLE(),
            GetN_EXT(),
            GetN_INT(),
            GetCOLONIA(),
            GetCP(),
            GetENTIDAD_NOMBRE_EMPR(),
            GetENTIDAD_CLAVE_EMPR(),
            GetMUNICIPIO_NOMBRE_EMPR(),
            GetMUNICIPIO_CLAVE_EMPR(),
            GetLATITUD_EMPR1(),
            GetLONGITUD_EMPR1(),
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
