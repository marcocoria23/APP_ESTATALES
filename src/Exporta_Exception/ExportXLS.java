package Exporta_Exception;

import java.awt.FileDialog;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.table.TableModel;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.IndexedColors;

public class ExportXLS {

    private static final int HSSF_MAX_ROWS = 65536;
    private static final int HSSF_MAX_COLS = 256;

    public void exportarExcel(JTable tabla) throws IOException {

        HSSFWorkbook libroExcel = null;
        FileOutputStream fos = null;

        try {
            libroExcel = new HSSFWorkbook();
            HSSFSheet hoja = libroExcel.createSheet("Datos");

            TableModel modelo = tabla.getModel();

            // Validaciones HSSF
            if (modelo.getColumnCount() > HSSF_MAX_COLS) {
                JOptionPane.showMessageDialog(null,
                        "El formato .xls solo permite hasta " + HSSF_MAX_COLS + " columnas",
                        "Límite .xls",
                        JOptionPane.ERROR_MESSAGE);
                return;
            }

            if (modelo.getRowCount() + 1 > HSSF_MAX_ROWS) {
                JOptionPane.showMessageDialog(null,
                        "El formato .xls solo permite hasta " + (HSSF_MAX_ROWS - 1) + " filas de datos",
                        "Límite .xls",
                        JOptionPane.ERROR_MESSAGE);
                return;
            }

            /* ===== ESTILOS ===== */
            HSSFCellStyle estiloEncabezado = libroExcel.createCellStyle();
            estiloEncabezado.setFillForegroundColor(IndexedColors.DARK_GREEN.getIndex());
            estiloEncabezado.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            estiloEncabezado.setBorderTop(HSSFCellStyle.BORDER_THIN);
            estiloEncabezado.setBorderBottom(HSSFCellStyle.BORDER_THIN);
            estiloEncabezado.setBorderLeft(HSSFCellStyle.BORDER_THIN);
            estiloEncabezado.setBorderRight(HSSFCellStyle.BORDER_THIN);

            HSSFFont font = libroExcel.createFont();
            font.setColor(IndexedColors.WHITE.getIndex());
            font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
            estiloEncabezado.setFont(font);

            HSSFCellStyle estiloDatos = libroExcel.createCellStyle();
            estiloDatos.setBorderTop(HSSFCellStyle.BORDER_THIN);
            estiloDatos.setBorderBottom(HSSFCellStyle.BORDER_THIN);
            estiloDatos.setBorderLeft(HSSFCellStyle.BORDER_THIN);
            estiloDatos.setBorderRight(HSSFCellStyle.BORDER_THIN);

            /* ===== ENCABEZADOS ===== */
            HSSFRow filaCabecera = hoja.createRow(0);
            for (int c = 0; c < modelo.getColumnCount(); c++) {
                HSSFCell celda = filaCabecera.createCell((short) c);
                celda.setCellValue(modelo.getColumnName(c));
                celda.setCellStyle(estiloEncabezado);
            }
            filaCabecera.setHeightInPoints(25);

            /* ===== DATOS ===== */
            for (int f = 0; f < modelo.getRowCount(); f++) {
                HSSFRow filaDatos = hoja.createRow(f + 1);
                for (int c = 0; c < modelo.getColumnCount(); c++) {
                    HSSFCell celda = filaDatos.createCell((short) c);
                    Object valor = modelo.getValueAt(f, c);
                    celda.setCellValue(valor == null ? "" : String.valueOf(valor));
                    celda.setCellStyle(estiloDatos);
                }
            }

            /* ===== AUTOSIZE ===== */
            for (int c = 0; c < modelo.getColumnCount(); c++) {
                hoja.autoSizeColumn((short) c);
            }

            /* ===== GUARDAR ===== */
            FileDialog d = new FileDialog(new JFrame(), "Guardar", FileDialog.SAVE);
            d.setFile("PLE_Exporta_Exception.xls");
            d.setVisible(true);

            if (d.getDirectory() != null && d.getFile() != null) {
                File out = new File(d.getDirectory(),
                        d.getFile().endsWith(".xls") ? d.getFile() : d.getFile() + ".xls");

                fos = new FileOutputStream(out);
                libroExcel.write(fos);

                JOptionPane.showMessageDialog(null, "Archivo guardado correctamente");
            }

        } finally {
            if (fos != null) try { fos.close(); } catch (IOException e) {}
        }
    }
}
