package plantillas;

import org.apache.poi.hssf.usermodel.*;

import java.awt.FileDialog;
import java.awt.Frame;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.List;
import javax.swing.JOptionPane;

public class ExportaCatalogosHSSF {

    private static final int HSSF_MAX_ROWS = 65536; // incluye header
    private static final int HSSF_MAX_COLS = 256;

    /**
     * Exporta catálogos a un .XLS usando FileDialog para elegir destino.
     * @param parentFrame Frame padre (puedes pasar new Frame() o tu JFrame con (Frame)SwingUtilities.getWindowAncestor(...))
     * @param con conexión JDBC a H2
     * @param tablas lista de tablas catálogo (ej. "TC_ACTOR", "TC_MUNICIPIO", etc.)
     */
    public static void exportarCatalogosConDialog(Frame parentFrame, Connection con, List<String> tablas) throws Exception {

        // 1) Elegir ruta
        String rutaXls = elegirRutaGuardarXls(parentFrame, "RALAB_CATALOGOS.xls");
        if (rutaXls == null) {
            // Usuario canceló
            return;
        }

        // 2) Crear workbook
        HSSFWorkbook wb = new HSSFWorkbook();

        // ===== Estilos (puro HSSF POI viejo) =====
        HSSFCellStyle headerStyle = wb.createCellStyle();
        HSSFFont headerFont = wb.createFont();
        headerFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
        headerStyle.setFont(headerFont);
        headerStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
        headerStyle.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);

        HSSFCellStyle cellStyle = wb.createCellStyle();
        cellStyle.setVerticalAlignment(HSSFCellStyle.VERTICAL_TOP);

        // 3) Exportar cada tabla a su sheet
        for (String tabla : tablas) {
            exportarTabla(con, wb, tabla, headerStyle, cellStyle);
        }

        // 4) Guardar (UNA sola vez)
        try (FileOutputStream fos = new FileOutputStream(rutaXls)) {
            wb.write(fos);
        }
    }

    private static String elegirRutaGuardarXls(Frame parent, String nombreSugerido) {
        FileDialog fd = new FileDialog(parent, "Guardar catálogos (XLS)", FileDialog.SAVE);
        fd.setFile(nombreSugerido); // sugerencia
        fd.setVisible(true);

        String dir = fd.getDirectory();
        String file = fd.getFile();

        if (dir == null || file == null) return null; // cancelado

        // Asegura extensión .xls
        if (!file.toLowerCase().endsWith(".xls")) {
            file = file + ".xls";
        }

        return new File(dir, file).getAbsolutePath();
    }

    private static void exportarTabla(Connection con,
                                      HSSFWorkbook wb,
                                      String tabla,
                                      HSSFCellStyle headerStyle,
                                      HSSFCellStyle cellStyle) throws SQLException {

        String sheetName = normalizaNombreSheet(tabla.replace("V3_","").replace("JL",""));
        HSSFSheet sheet = wb.createSheet(sheetName);

        // Si tus tablas están en schema REL_2021, usa:
        // String sql = "SELECT * FROM REL_2021." + tabla + " ORDER BY 1";
        String sql = "SELECT * FROM " + tabla + " ORDER BY 1";

        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            if (colCount > HSSF_MAX_COLS) {
                throw new SQLException("La tabla " + tabla + " tiene " + colCount +
                        " columnas y HSSF soporta máximo " + HSSF_MAX_COLS);
            }

            // ===== Header =====
            HSSFRow header = sheet.createRow(0);
            for (int c = 1; c <= colCount; c++) {
                HSSFCell cell = header.createCell((short) (c - 1));
                cell.setCellValue(md.getColumnLabel(c));
                cell.setCellStyle(headerStyle);
            }

            // ===== Data =====
            int rowIdx = 1;
            while (rs.next()) {
                if (rowIdx >= HSSF_MAX_ROWS) break; // límite .xls

                HSSFRow row = sheet.createRow(rowIdx++);
                for (int c = 1; c <= colCount; c++) {
                    Object v = rs.getObject(c);
                    HSSFCell cell = row.createCell((short) (c - 1));

                    if (v == null) {
                        cell.setCellType(HSSFCell.CELL_TYPE_BLANK);
                    } else if (v instanceof Number) {
                        cell.setCellValue(((Number) v).doubleValue());
                    } else if (v instanceof java.sql.Date) {
                        cell.setCellValue(new java.util.Date(((java.sql.Date) v).getTime()));
                    } else if (v instanceof java.sql.Timestamp) {
                        cell.setCellValue(new java.util.Date(((java.sql.Timestamp) v).getTime()));
                    } else if (v instanceof Boolean) {
                        cell.setCellValue(((Boolean) v).booleanValue());
                    } else {
                        cell.setCellValue(v.toString());
                    }

                    cell.setCellStyle(cellStyle);
                }
            }

            // Freeze header
            sheet.createFreezePane(0, 1);

            // Auto-size (catálogos chicos OK)
            for (int i = 0; i < colCount; i++) {
                sheet.autoSizeColumn((short) i);
                int w = sheet.getColumnWidth((short) i);
                int max = 12000;
                if (w > max) sheet.setColumnWidth((short) i, (short) max);
            }
        }
    }

    private static String normalizaNombreSheet(String tabla) {
        String name = tabla.replaceAll("[\\\\/?*\\[\\]:]", "_");
        if (name.length() > 31) name = name.substring(0, 31);
        return name;
    }
}
