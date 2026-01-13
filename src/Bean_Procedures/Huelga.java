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
public class Huelga  extends Bean implements SQLData {

    private String SQLTypeName; 
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String FECHA_APERTURA_EXPEDIENTE;
    private String TIPO_ASUNTO;
    private String RAMA_INDUS_INVOLUCRAD;
    private String SECTOR_RAMA;
    private String SUBSECTOR_RAMA;
    private String ENTIDAD_CLAVE;
    private String ENTIDAD_NOMBRE;
    private String MUNICIPIO_CLAVE;
    private String MUNICIPIO_NOMBRE;
    private String FIRMA_CONTRATO;
    private String REVISION_CONTRATO;
    private String INCUMPLIM_CONTRATO;
    private String REVISION_SALARIO;
    private String REPARTO_UTILIDADES;
    private String APOYO_OTRA_HUELGA;
    private String DESEQUILIBRIO_FAC_PROD;
    private String OTRO_MOTIVO;
    private String ESPECIFIQUE_MOTIVO;
    private String INCOMPETENCIA;
    private String TIPO_INCOMPETENCIA;
    private String ESPECIFIQUE_INCOMP;
    private String FECHA_PRESENTA_PETIC;
    private String CANTIDAD_ACTORES;
    private String CANTIDAD_DEMANDADOS;
    private String EMPLAZAMIENTO_HUELGA;
    private String FECHA_EMPLAZAMIENTO;
    private String PREHUELGA;
    private String AUDIENCIA_CONCILIACION;
    private String FECHA_AUDIENCIA;
    private String ESTALLAMIENTO_HUELGA;
    private String DECLARA_LICITUD_HUELGA;
    private String DECLARA_EXISTEN_HUELGA;
    private String ESTATUS_EXPEDIENTE;
    private String FECHA_ACTO_PROCESAL;
    private String FASE_SOLI_EXPEDIENTE;
    private String FORMA_SOLUCION_EMPLAZ;
    private String ESPECIFI_FORMA_EMPLAZ;
    private String FECHA_RESOLU_EMPLAZ;
    private String INCREMENTO_SOLICITADO;
    private String INCREMENTO_OTORGADO;
    private String FORMA_SOLUCION_HUELGA;
    private String ESPECIFI_FORMA_HUELGA;
    private String FECHA_RESOLU_HUELGA;
    private String TIPO_SENTENCIA;
    private String FECHA_ESTALLAM_HUELGA;
    private String FECHA_LEVANT_HUELGA;
    private String DIAS_HUELGA;
    private String MONTO_ESTIPULADO;
    private String SALARIOS_CAIDOS;
    private String COMENTARIOS;
    private String PERIODO;


    
    
    public Huelga(){   
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
public String GetFECHA_APERTURA_EXPEDIENTE() {
        return FECHA_APERTURA_EXPEDIENTE;
    }

    public void SetFECHA_APERTURA_EXPEDIENTE(String FECHA_APERTURA_EXPEDIENTE) {
        this.FECHA_APERTURA_EXPEDIENTE = FECHA_APERTURA_EXPEDIENTE;
    }
public String GetTIPO_ASUNTO() {
        return TIPO_ASUNTO;
    }

    public void SetTIPO_ASUNTO(String TIPO_ASUNTO) {
        this.TIPO_ASUNTO = TIPO_ASUNTO;
    }
public String GetRAMA_INDUS_INVOLUCRAD() {
        return RAMA_INDUS_INVOLUCRAD;
    }

    public void SetRAMA_INDUS_INVOLUCRAD(String RAMA_INDUS_INVOLUCRAD) {
        this.RAMA_INDUS_INVOLUCRAD = RAMA_INDUS_INVOLUCRAD;
    }
public String GetSECTOR_RAMA() {
        return SECTOR_RAMA;
    }

    public void SetSECTOR_RAMA(String SECTOR_RAMA) {
        this.SECTOR_RAMA = SECTOR_RAMA;
    }
public String GetSUBSECTOR_RAMA() {
        return SUBSECTOR_RAMA;
    }

    public void SetSUBSECTOR_RAMA(String SUBSECTOR_RAMA) {
        this.SUBSECTOR_RAMA = SUBSECTOR_RAMA;
    }
public String GetENTIDAD_CLAVE() {
        return ENTIDAD_CLAVE;
    }

    public void SetENTIDAD_CLAVE(String ENTIDAD_CLAVE) {
        this.ENTIDAD_CLAVE = ENTIDAD_CLAVE;
    }
public String GetENTIDAD_NOMBRE() {
        return ENTIDAD_NOMBRE;
    }

    public void SetENTIDAD_NOMBRE(String ENTIDAD_NOMBRE) {
        this.ENTIDAD_NOMBRE = ENTIDAD_NOMBRE;
    }
public String GetMUNICIPIO_CLAVE() {
        return MUNICIPIO_CLAVE;
    }

    public void SetMUNICIPIO_CLAVE(String MUNICIPIO_CLAVE) {
        this.MUNICIPIO_CLAVE = MUNICIPIO_CLAVE;
    }
public String GetMUNICIPIO_NOMBRE() {
        return MUNICIPIO_NOMBRE;
    }

    public void SetMUNICIPIO_NOMBRE(String MUNICIPIO_NOMBRE) {
        this.MUNICIPIO_NOMBRE = MUNICIPIO_NOMBRE;
    }
public String GetFIRMA_CONTRATO() {
        return FIRMA_CONTRATO;
    }

    public void SetFIRMA_CONTRATO(String FIRMA_CONTRATO) {
        this.FIRMA_CONTRATO = FIRMA_CONTRATO;
    }
public String GetREVISION_CONTRATO() {
        return REVISION_CONTRATO;
    }

    public void SetREVISION_CONTRATO(String REVISION_CONTRATO) {
        this.REVISION_CONTRATO = REVISION_CONTRATO;
    }
public String GetINCUMPLIM_CONTRATO() {
        return INCUMPLIM_CONTRATO;
    }

    public void SetINCUMPLIM_CONTRATO(String INCUMPLIM_CONTRATO) {
        this.INCUMPLIM_CONTRATO = INCUMPLIM_CONTRATO;
    }
public String GetREVISION_SALARIO() {
        return REVISION_SALARIO;
    }

    public void SetREVISION_SALARIO(String REVISION_SALARIO) {
        this.REVISION_SALARIO = REVISION_SALARIO;
    }
public String GetREPARTO_UTILIDADES() {
        return REPARTO_UTILIDADES;
    }

    public void SetREPARTO_UTILIDADES(String REPARTO_UTILIDADES) {
        this.REPARTO_UTILIDADES = REPARTO_UTILIDADES;
    }
public String GetAPOYO_OTRA_HUELGA() {
        return APOYO_OTRA_HUELGA;
    }

    public void SetAPOYO_OTRA_HUELGA(String APOYO_OTRA_HUELGA) {
        this.APOYO_OTRA_HUELGA = APOYO_OTRA_HUELGA;
    }
public String GetDESEQUILIBRIO_FAC_PROD() {
        return DESEQUILIBRIO_FAC_PROD;
    }

    public void SetDESEQUILIBRIO_FAC_PROD(String DESEQUILIBRIO_FAC_PROD) {
        this.DESEQUILIBRIO_FAC_PROD = DESEQUILIBRIO_FAC_PROD;
    }
public String GetOTRO_MOTIVO() {
        return OTRO_MOTIVO;
    }

    public void SetOTRO_MOTIVO(String OTRO_MOTIVO) {
        this.OTRO_MOTIVO = OTRO_MOTIVO;
    }
public String GetESPECIFIQUE_MOTIVO() {
        return ESPECIFIQUE_MOTIVO;
    }

    public void SetESPECIFIQUE_MOTIVO(String ESPECIFIQUE_MOTIVO) {
        this.ESPECIFIQUE_MOTIVO = ESPECIFIQUE_MOTIVO;
    }
public String GetINCOMPETENCIA() {
        return INCOMPETENCIA;
    }

    public void SetINCOMPETENCIA(String INCOMPETENCIA) {
        this.INCOMPETENCIA = INCOMPETENCIA;
    }
public String GetTIPO_INCOMPETENCIA() {
        return TIPO_INCOMPETENCIA;
    }

    public void SetTIPO_INCOMPETENCIA(String TIPO_INCOMPETENCIA) {
        this.TIPO_INCOMPETENCIA = TIPO_INCOMPETENCIA;
    }
public String GetESPECIFIQUE_INCOMP() {
        return ESPECIFIQUE_INCOMP;
    }

    public void SetESPECIFIQUE_INCOMP(String ESPECIFIQUE_INCOMP) {
        this.ESPECIFIQUE_INCOMP = ESPECIFIQUE_INCOMP;
    }
public String GetFECHA_PRESENTA_PETIC() {
        return FECHA_PRESENTA_PETIC;
    }

    public void SetFECHA_PRESENTA_PETIC(String FECHA_PRESENTA_PETIC) {
        this.FECHA_PRESENTA_PETIC = FECHA_PRESENTA_PETIC;
    }
public String GetCANTIDAD_ACTORES() {
        return CANTIDAD_ACTORES;
    }

    public void SetCANTIDAD_ACTORES(String CANTIDAD_ACTORES) {
        this.CANTIDAD_ACTORES = CANTIDAD_ACTORES;
    }
public String GetCANTIDAD_DEMANDADOS() {
        return CANTIDAD_DEMANDADOS;
    }

    public void SetCANTIDAD_DEMANDADOS(String CANTIDAD_DEMANDADOS) {
        this.CANTIDAD_DEMANDADOS = CANTIDAD_DEMANDADOS;
    }
public String GetEMPLAZAMIENTO_HUELGA() {
        return EMPLAZAMIENTO_HUELGA;
    }

    public void SetEMPLAZAMIENTO_HUELGA(String EMPLAZAMIENTO_HUELGA) {
        this.EMPLAZAMIENTO_HUELGA = EMPLAZAMIENTO_HUELGA;
    }
public String GetFECHA_EMPLAZAMIENTO() {
        return FECHA_EMPLAZAMIENTO;
    }

    public void SetFECHA_EMPLAZAMIENTO(String FECHA_EMPLAZAMIENTO) {
        this.FECHA_EMPLAZAMIENTO = FECHA_EMPLAZAMIENTO;
    }
public String GetPREHUELGA() {
        return PREHUELGA;
    }

    public void SetPREHUELGA(String PREHUELGA) {
        this.PREHUELGA = PREHUELGA;
    }
public String GetAUDIENCIA_CONCILIACION() {
        return AUDIENCIA_CONCILIACION;
    }

    public void SetAUDIENCIA_CONCILIACION(String AUDIENCIA_CONCILIACION) {
        this.AUDIENCIA_CONCILIACION = AUDIENCIA_CONCILIACION;
    }
public String GetFECHA_AUDIENCIA() {
        return FECHA_AUDIENCIA;
    }

    public void SetFECHA_AUDIENCIA(String FECHA_AUDIENCIA) {
        this.FECHA_AUDIENCIA = FECHA_AUDIENCIA;
    }
public String GetESTALLAMIENTO_HUELGA() {
        return ESTALLAMIENTO_HUELGA;
    }

    public void SetESTALLAMIENTO_HUELGA(String ESTALLAMIENTO_HUELGA) {
        this.ESTALLAMIENTO_HUELGA = ESTALLAMIENTO_HUELGA;
    }
public String GetDECLARA_LICITUD_HUELGA() {
        return DECLARA_LICITUD_HUELGA;
    }

    public void SetDECLARA_LICITUD_HUELGA(String DECLARA_LICITUD_HUELGA) {
        this.DECLARA_LICITUD_HUELGA = DECLARA_LICITUD_HUELGA;
    }
public String GetDECLARA_EXISTEN_HUELGA() {
        return DECLARA_EXISTEN_HUELGA;
    }

    public void SetDECLARA_EXISTEN_HUELGA(String DECLARA_EXISTEN_HUELGA) {
        this.DECLARA_EXISTEN_HUELGA = DECLARA_EXISTEN_HUELGA;
    }
public String GetESTATUS_EXPEDIENTE() {
        return ESTATUS_EXPEDIENTE;
    }

    public void SetESTATUS_EXPEDIENTE(String ESTATUS_EXPEDIENTE) {
        this.ESTATUS_EXPEDIENTE = ESTATUS_EXPEDIENTE;
    }
public String GetFECHA_ACTO_PROCESAL() {
        return FECHA_ACTO_PROCESAL;
    }

    public void SetFECHA_ACTO_PROCESAL(String FECHA_ACTO_PROCESAL) {
        this.FECHA_ACTO_PROCESAL = FECHA_ACTO_PROCESAL;
    }
public String GetFASE_SOLI_EXPEDIENTE() {
        return FASE_SOLI_EXPEDIENTE;
    }

    public void SetFASE_SOLI_EXPEDIENTE(String FASE_SOLI_EXPEDIENTE) {
        this.FASE_SOLI_EXPEDIENTE = FASE_SOLI_EXPEDIENTE;
    }
public String GetFORMA_SOLUCION_EMPLAZ() {
        return FORMA_SOLUCION_EMPLAZ;
    }

    public void SetFORMA_SOLUCION_EMPLAZ(String FORMA_SOLUCION_EMPLAZ) {
        this.FORMA_SOLUCION_EMPLAZ = FORMA_SOLUCION_EMPLAZ;
    }
public String GetESPECIFI_FORMA_EMPLAZ() {
        return ESPECIFI_FORMA_EMPLAZ;
    }

    public void SetESPECIFI_FORMA_EMPLAZ(String ESPECIFI_FORMA_EMPLAZ) {
        this.ESPECIFI_FORMA_EMPLAZ = ESPECIFI_FORMA_EMPLAZ;
    }
public String GetFECHA_RESOLU_EMPLAZ() {
        return FECHA_RESOLU_EMPLAZ;
    }

    public void SetFECHA_RESOLU_EMPLAZ(String FECHA_RESOLU_EMPLAZ) {
        this.FECHA_RESOLU_EMPLAZ = FECHA_RESOLU_EMPLAZ;
    }
public String GetINCREMENTO_SOLICITADO() {
        return INCREMENTO_SOLICITADO;
    }

    public void SetINCREMENTO_SOLICITADO(String INCREMENTO_SOLICITADO) {
        this.INCREMENTO_SOLICITADO = INCREMENTO_SOLICITADO;
    }
public String GetINCREMENTO_OTORGADO() {
        return INCREMENTO_OTORGADO;
    }

    public void SetINCREMENTO_OTORGADO(String INCREMENTO_OTORGADO) {
        this.INCREMENTO_OTORGADO = INCREMENTO_OTORGADO;
    }
public String GetFORMA_SOLUCION_HUELGA() {
        return FORMA_SOLUCION_HUELGA;
    }

    public void SetFORMA_SOLUCION_HUELGA(String FORMA_SOLUCION_HUELGA) {
        this.FORMA_SOLUCION_HUELGA = FORMA_SOLUCION_HUELGA;
    }
public String GetESPECIFI_FORMA_HUELGA() {
        return ESPECIFI_FORMA_HUELGA;
    }

    public void SetESPECIFI_FORMA_HUELGA(String ESPECIFI_FORMA_HUELGA) {
        this.ESPECIFI_FORMA_HUELGA = ESPECIFI_FORMA_HUELGA;
    }
public String GetFECHA_RESOLU_HUELGA() {
        return FECHA_RESOLU_HUELGA;
    }

    public void SetFECHA_RESOLU_HUELGA(String FECHA_RESOLU_HUELGA) {
        this.FECHA_RESOLU_HUELGA = FECHA_RESOLU_HUELGA;
    }
public String GetTIPO_SENTENCIA() {
        return TIPO_SENTENCIA;
    }

    public void SetTIPO_SENTENCIA(String TIPO_SENTENCIA) {
        this.TIPO_SENTENCIA = TIPO_SENTENCIA;
    }
public String GetFECHA_ESTALLAM_HUELGA() {
        return FECHA_ESTALLAM_HUELGA;
    }

    public void SetFECHA_ESTALLAM_HUELGA(String FECHA_ESTALLAM_HUELGA) {
        this.FECHA_ESTALLAM_HUELGA = FECHA_ESTALLAM_HUELGA;
    }
public String GetFECHA_LEVANT_HUELGA() {
        return FECHA_LEVANT_HUELGA;
    }

    public void SetFECHA_LEVANT_HUELGA(String FECHA_LEVANT_HUELGA) {
        this.FECHA_LEVANT_HUELGA = FECHA_LEVANT_HUELGA;
    }
public String GetDIAS_HUELGA() {
        return DIAS_HUELGA;
    }

    public void SetDIAS_HUELGA(String DIAS_HUELGA) {
        this.DIAS_HUELGA = DIAS_HUELGA;
    }
public String GetMONTO_ESTIPULADO() {
        return MONTO_ESTIPULADO;
    }

    public void SetMONTO_ESTIPULADO(String MONTO_ESTIPULADO) {
        this.MONTO_ESTIPULADO = MONTO_ESTIPULADO;
    }
public String GetSALARIOS_CAIDOS() {
        return SALARIOS_CAIDOS;
    }

    public void SetSALARIOS_CAIDOS(String SALARIOS_CAIDOS) {
        this.SALARIOS_CAIDOS = SALARIOS_CAIDOS;
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
         GetFECHA_APERTURA_EXPEDIENTE(),
         GetTIPO_ASUNTO(),
         GetRAMA_INDUS_INVOLUCRAD(),
         GetSECTOR_RAMA(),
         GetSUBSECTOR_RAMA(),
         GetENTIDAD_CLAVE(),
         GetENTIDAD_NOMBRE(),
         GetMUNICIPIO_CLAVE(),
         GetMUNICIPIO_NOMBRE(),
         GetFIRMA_CONTRATO(),
         GetREVISION_CONTRATO(),
         GetINCUMPLIM_CONTRATO(),
         GetREVISION_SALARIO(),
         GetREPARTO_UTILIDADES(),
         GetAPOYO_OTRA_HUELGA(),
         GetDESEQUILIBRIO_FAC_PROD(),
         GetOTRO_MOTIVO(),
         GetESPECIFIQUE_MOTIVO(),
         GetINCOMPETENCIA(),
         GetTIPO_INCOMPETENCIA(),
         GetESPECIFIQUE_INCOMP(),
         GetFECHA_PRESENTA_PETIC(),
         GetCANTIDAD_ACTORES(),
         GetCANTIDAD_DEMANDADOS(),
         GetEMPLAZAMIENTO_HUELGA(),
         GetFECHA_EMPLAZAMIENTO(),
         GetPREHUELGA(),
         GetAUDIENCIA_CONCILIACION(),
         GetFECHA_AUDIENCIA(),
         GetESTALLAMIENTO_HUELGA(),
         GetDECLARA_LICITUD_HUELGA(),
         GetDECLARA_EXISTEN_HUELGA(),
         GetESTATUS_EXPEDIENTE(),
         GetFECHA_ACTO_PROCESAL(),
         GetFASE_SOLI_EXPEDIENTE(),
         GetFORMA_SOLUCION_EMPLAZ(),
         GetESPECIFI_FORMA_EMPLAZ(),
         GetFECHA_RESOLU_EMPLAZ(),
         GetINCREMENTO_SOLICITADO(),
         GetINCREMENTO_OTORGADO(),
         GetFORMA_SOLUCION_HUELGA(),
         GetESPECIFI_FORMA_HUELGA(),
         GetFECHA_RESOLU_HUELGA(),
         GetTIPO_SENTENCIA(),
         GetFECHA_ESTALLAM_HUELGA(),
         GetFECHA_LEVANT_HUELGA(),
         GetDIAS_HUELGA(),
         GetMONTO_ESTIPULADO(),
         GetSALARIOS_CAIDOS(),
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
