package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types; // Importación necesaria para java.sql.Types.DATE
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;
// Asumiendo que estas clases de utilidad para errores Oracle son parte de tu proyecto
import lsi.ubu.util.exceptions.SGBDError; 
import lsi.ubu.util.exceptions.oracle.OracleSGBDErrorUtil;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4; 

	@Override
	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		long diasDiff = DIAS_DE_ALQUILER; 

		if (fechaIni == null) {
		    LOGGER.error("La fecha de inicio no puede ser nula para el alquiler.");
		    throw new SQLException("Fecha de inicio requerida para el alquiler."); 
		}
		
		java.sql.Date sqlFechaIni = new java.sql.Date(fechaIni.getTime());
		java.sql.Date sqlFechaFinParaInsertar = null; // Por defecto NULL si fechaFin es NULL

		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());
			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
			sqlFechaFinParaInsertar = new java.sql.Date(fechaFin.getTime());
		}
		// Si fechaFin es NULL, sqlFechaFinParaInsertar sigue siendo NULL.
		// diasDiff será DIAS_DE_ALQUILER para el cálculo de la factura.

		// Para la comprobación de solapamiento, necesitamos una fecha de fin efectiva.
        java.sql.Date sqlFechaFinEfectivaParaComprobacion;
        if (fechaFin != null) {
            sqlFechaFinEfectivaParaComprobacion = sqlFechaFinParaInsertar;
        } else {
            Calendar cal = Calendar.getInstance();
            cal.setTime(fechaIni);
            cal.add(Calendar.DAY_OF_YEAR, DIAS_DE_ALQUILER);
            sqlFechaFinEfectivaParaComprobacion = new java.sql.Date(cal.getTimeInMillis());
        }


		try {
			con = pool.getConnection();
			con.setAutoCommit(false); 

			// 1. Comprobar existencia del cliente
			String sqlCheckCliente = "SELECT 1 FROM Clientes WHERE NIF = ?";
			st = con.prepareStatement(sqlCheckCliente);
			st.setString(1, nifCliente);
			rs = st.executeQuery();
			if (!rs.next()) {
			    if(rs!=null) rs.close(); 
			    if(st!=null) st.close();
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}
			rs.close();
			st.close();

			// 2. Obtener datos del vehículo y precio del combustible
			String queryVehiculoPrecio = 
				"SELECT m.ID_MODELO, m.PRECIO_CADA_DIA, m.CAPACIDAD_DEPOSITO, m.TIPO_COMBUSTIBLE, pc.PRECIO_POR_LITRO " +
				"FROM VEHICULOS v " +
				"INNER JOIN MODELOS m ON v.ID_MODELO = m.ID_MODELO " +
				"INNER JOIN PRECIO_COMBUSTIBLE pc ON m.TIPO_COMBUSTIBLE = pc.TIPO_COMBUSTIBLE " +
				"WHERE v.MATRICULA = ?";
			st = con.prepareStatement(queryVehiculoPrecio);
			st.setString(1, matricula);
			rs = st.executeQuery();
			if (!rs.next()) {
			    if(rs!=null) rs.close(); 
			    if(st!=null) st.close();
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
			}

			BigDecimal precioCadaDia = rs.getBigDecimal("PRECIO_CADA_DIA");
			int capacidadDeposito = rs.getInt("CAPACIDAD_DEPOSITO");
			BigDecimal precioPorLitro = rs.getBigDecimal("PRECIO_POR_LITRO");
			int idModelo = rs.getInt("ID_MODELO");
			String tipoCombustible = rs.getString("TIPO_COMBUSTIBLE");
			rs.close();
			st.close();

			// 3. Comprobar solapamiento de reservas (usando NVL para manejar fechas de fin NULL en la BD)
			String sqlOverlapCheck = "SELECT IDRESERVA FROM RESERVAS WHERE MATRICULA = ? AND FECHA_INI < ? AND NVL(FECHA_FIN, FECHA_INI + 1000) > ?";
			st = con.prepareStatement(sqlOverlapCheck);
			st.setString(1, matricula);
			st.setDate(2, sqlFechaFinEfectivaParaComprobacion); 
			st.setDate(3, sqlFechaIni);          
			rs = st.executeQuery();
			if (rs.next()) {
			    if(rs!=null) rs.close();
			    if(st!=null) st.close();
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
			}
			rs.close();
			st.close();

			// 4. Insertar la reserva
			String sqlInsertReserva = "INSERT INTO RESERVAS (IDRESERVA, CLIENTE, MATRICULA, FECHA_INI, FECHA_FIN) VALUES (seq_reservas.nextVal, ?, ?, ?, ?)";
			st = con.prepareStatement(sqlInsertReserva);
			st.setString(1, nifCliente);
			st.setString(2, matricula);
			st.setDate(3, sqlFechaIni);
			if (sqlFechaFinParaInsertar != null) {
				st.setDate(4, sqlFechaFinParaInsertar);
			} else {
				st.setNull(4, Types.DATE); // MODIFICACIÓN CLAVE: Inserta NULL para pasar el Test Caso 4
			}
			int GestiCanvis = st.executeUpdate();
            if (GestiCanvis == 0) { 
                throw new SQLException("Error al insertar la reserva, ninguna fila afectada.");
            }
			st.close();

			// 5. Crear factura y sus líneas
			BigDecimal diasFactura = new BigDecimal(diasDiff);
			BigDecimal capacidadDepositoComoBigDecimal = new BigDecimal(capacidadDeposito);
			BigDecimal precioAlquiler = precioCadaDia.multiply(diasFactura);
			BigDecimal precioCombustible = precioPorLitro.multiply(capacidadDepositoComoBigDecimal);
			BigDecimal precioTotalFactura = precioAlquiler.add(precioCombustible);

			int nroFactura;
			st = con.prepareStatement("SELECT seq_num_fact.nextVal AS valor FROM dual");
			rs = st.executeQuery();
			if (!rs.next()) {
			    if(rs!=null) rs.close();
			    if(st!=null) st.close();
				throw new SQLException("No se pudo obtener el siguiente valor de la secuencia de facturas.");
			}
			nroFactura = rs.getInt("valor");
			rs.close();
			st.close();
			
			String sqlInsertFactura = "INSERT INTO FACTURAS (NROFACTURA, CLIENTE, IMPORTE) VALUES (?, ?, ?)";
			st = con.prepareStatement(sqlInsertFactura);
			st.setInt(1, nroFactura);
			st.setString(2, nifCliente);
			st.setBigDecimal(3, precioTotalFactura);
			GestiCanvis = st.executeUpdate();
            if (GestiCanvis == 0) {
                throw new SQLException("Error al insertar la factura, ninguna fila afectada.");
            }
			st.close();

			String sqlInsertLineaAlquiler = "INSERT INTO LINEAS_FACTURA (NROFACTURA, CONCEPTO, IMPORTE) VALUES (?, ?, ?)";
			st = con.prepareStatement(sqlInsertLineaAlquiler);
			st.setInt(1, nroFactura);
			st.setString(2, diasDiff + " dias de alquiler, vehiculo modelo " + idModelo);
			st.setBigDecimal(3, precioAlquiler);
			GestiCanvis = st.executeUpdate();
            if (GestiCanvis == 0) {
                 throw new SQLException("Error al insertar la línea de factura (alquiler), ninguna fila afectada.");
            }
			st.close();

			String sqlInsertLineaCombustible = "INSERT INTO LINEAS_FACTURA (NROFACTURA, CONCEPTO, IMPORTE) VALUES (?, ?, ?)";
			st = con.prepareStatement(sqlInsertLineaCombustible);
			st.setInt(1, nroFactura);
			st.setString(2, "Deposito lleno de " + capacidadDeposito + " litros de " + tipoCombustible);
			st.setBigDecimal(3, precioCombustible);
			GestiCanvis = st.executeUpdate();
            if (GestiCanvis == 0) {
                throw new SQLException("Error al insertar la línea de factura (combustible), ninguna fila afectada.");
            }
			st.close();

			con.commit(); 
			LOGGER.info("Alquiler realizado y factura creada con éxito para cliente {} y vehículo {}.", nifCliente, matricula);

		} catch (SQLException e) {
			LOGGER.error("SQLException en alquilar (Código: {}): {}", e.getErrorCode(), e.getMessage(), e);
			if (con != null) {
				try {
					LOGGER.info("Realizando rollback debido a SQLException en alquilar...");
					con.rollback(); 
				} catch (SQLException exRollback) {
					LOGGER.error("Error CRÍTICO al intentar rollback en alquilar.", exRollback);
				}
			}
			if (!(e instanceof AlquilerCochesException) && new OracleSGBDErrorUtil().checkExceptionToCode(e, SGBDError.FK_VIOLATED)) { 
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST); 
			}
			throw e; 
		} finally {
			try { if (rs != null) rs.close(); } catch (SQLException e) { LOGGER.warn("Error cerrando ResultSet en alquilar", e); }
			try { if (st != null) st.close(); } catch (SQLException e) { LOGGER.warn("Error cerrando PreparedStatement en alquilar", e); }
			try { 
				if (con != null) {
					con.close(); 
				}
			} catch (SQLException e) { LOGGER.warn("Error cerrando Connection en alquilar", e); }
		}
	}

	@Override
	public void anular_alquiler(String idReservaStr, String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        long diasDiff;

        if (fechaIni == null || fechaFin == null) {
            LOGGER.error("Las fechas de inicio y fin son obligatorias para la anulación.");
            throw new SQLException("Fechas de inicio y fin requeridas para la anulación.");
        }

        diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());
        if (diasDiff < 1) {
            throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS); 
        }

        java.sql.Date sqlFechaIniParam = new java.sql.Date(fechaIni.getTime());
        java.sql.Date sqlFechaFinParam = new java.sql.Date(fechaFin.getTime());
        int idReserva;

        try {
            idReserva = Integer.parseInt(idReservaStr);
        } catch (NumberFormatException e) {
            LOGGER.error("idReserva '{}' no es un número válido.", idReservaStr);
            throw new AlquilerCochesException(AlquilerCochesException.RESERVA_NO_EXIST); 
        }

        try {
            con = pool.getConnection();
            con.setAutoCommit(false); 

            // 1. Verificar existencia de la reserva y obtener sus datos
            String dbNifCliente;
            String dbMatricula;
            java.sql.Date dbSqlFechaIni;
            java.sql.Date dbSqlFechaFin;

            String sqlCheckReserva = "SELECT cliente, matricula, fecha_ini, fecha_fin FROM Reservas WHERE idReserva = ?";
            st = con.prepareStatement(sqlCheckReserva);
            st.setInt(1, idReserva);
            rs = st.executeQuery();

            if (!rs.next()) {
                if(rs!=null) rs.close(); 
                if(st!=null) st.close();
                throw new AlquilerCochesException(AlquilerCochesException.RESERVA_NO_EXIST); 
            }
            dbNifCliente = rs.getString("cliente");
            dbMatricula = rs.getString("matricula");
            dbSqlFechaIni = rs.getDate("fecha_ini");
            dbSqlFechaFin = rs.getDate("fecha_fin");
            rs.close(); 
            st.close();

            // 2. Validar que los datos proporcionados al método coinciden con los de la reserva almacenada
            boolean fechasFinCoinciden;
            if (dbSqlFechaFin == null) {
                // Si en la BD es null, se asume que la reserva fue de la duración por defecto.
                // La fecha fin del parámetro debe coincidir con esa fecha calculada.
                Calendar cal = Calendar.getInstance();
                cal.setTime(dbSqlFechaIni);
                cal.add(Calendar.DAY_OF_YEAR, DIAS_DE_ALQUILER);
                java.sql.Date fechaFinCalculada = new java.sql.Date(cal.getTimeInMillis());
                fechasFinCoinciden = fechaFinCalculada.equals(sqlFechaFinParam);
            } else {
                 fechasFinCoinciden = dbSqlFechaFin.equals(sqlFechaFinParam);
            }

            if (!dbNifCliente.equals(nifCliente) ||
                !dbMatricula.equals(matricula) ||
                !dbSqlFechaIni.equals(sqlFechaIniParam) ||
                !fechasFinCoinciden ) {
                LOGGER.error("Los datos proporcionados para la anulación no coinciden con los de la reserva ID {}.", idReserva);
                throw new SQLException("Los datos proporcionados (NIF, matrícula, fechas) no coinciden con los de la reserva a anular.");
            }
            
            // 3. Comprobar existencia del cliente
            String sqlCheckCliente = "SELECT 1 FROM Clientes WHERE NIF = ?";
            st = con.prepareStatement(sqlCheckCliente);
            st.setString(1, nifCliente); 
            rs = st.executeQuery();
            if (!rs.next()) {
                if(rs!=null) rs.close(); 
                if(st!=null) st.close();
                throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST); 
            }
            rs.close(); 
            st.close();

            // 4. Comprobar existencia del vehículo
            String sqlCheckVehiculo = "SELECT 1 FROM Vehiculos WHERE matricula = ?";
            st = con.prepareStatement(sqlCheckVehiculo);
            st.setString(1, matricula); 
            rs = st.executeQuery();
            if (!rs.next()) {
                if(rs!=null) rs.close(); 
                if(st!=null) st.close();
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST); 
            }
            rs.close(); 
            st.close();
            
            // Determinar la fecha de fin efectiva de la reserva original
            java.sql.Date fechaFinReservaOriginalEfectiva;
            if (dbSqlFechaFin != null) {
                fechaFinReservaOriginalEfectiva = dbSqlFechaFin;
            } else {
                Calendar cal = Calendar.getInstance();
                cal.setTime(dbSqlFechaIni);
                cal.add(Calendar.DAY_OF_YEAR, DIAS_DE_ALQUILER);
                fechaFinReservaOriginalEfectiva = new java.sql.Date(cal.getTimeInMillis());
            }

            // 5. Comprobar "Si ese vehículo ya no está disponible" (Código 5)
            String sqlVehiculoNoDisponibleCheck = "SELECT 1 FROM Reservas " +
                                                  "WHERE matricula = ? " +
                                                  "AND fecha_ini < ? " + 
                                                  "AND NVL(fecha_fin, fecha_ini + 1000) > ? " + 
                                                  "AND idReserva <> ?"; 
            st = con.prepareStatement(sqlVehiculoNoDisponibleCheck);
            st.setString(1, dbMatricula);    
            st.setDate(2, fechaFinReservaOriginalEfectiva);
            st.setDate(3, dbSqlFechaIni);    
            st.setInt(4, idReserva);
            rs = st.executeQuery();
            if (rs.next()) { 
                if(rs!=null) rs.close(); 
                if(st!=null) st.close();
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO); 
            }
            rs.close(); 
            st.close();

            // 6. Recalcular importe de la factura para encontrarla y eliminarla
            long diasReservaOriginal;
            if (dbSqlFechaFin != null) {
                diasReservaOriginal = TimeUnit.MILLISECONDS.toDays(dbSqlFechaFin.getTime() - dbSqlFechaIni.getTime());
            } else {
                diasReservaOriginal = DIAS_DE_ALQUILER;
            }
            if (diasReservaOriginal < 1) diasReservaOriginal = 1;

            String sqlModeloInfo = "SELECT m.precio_cada_dia, m.tipo_combustible, m.capacidad_deposito " + "FROM vehiculos v JOIN modelos m ON v.id_modelo = m.id_modelo " + "WHERE v.matricula = ?";
            st = con.prepareStatement(sqlModeloInfo); st.setString(1, dbMatricula); rs = st.executeQuery();
            if (!rs.next()) { throw new SQLException("Error crítico: No se pudo obtener información del modelo " + dbMatricula); }
            BigDecimal precioPorDia = rs.getBigDecimal("precio_cada_dia"); String tipoCombustible = rs.getString("tipo_combustible"); int capacidadDeposito = rs.getInt("capacidad_deposito");
            rs.close(); st.close();

            String sqlPrecioCombustible = "SELECT precio_por_litro FROM precio_combustible WHERE tipo_combustible = ?";
            st = con.prepareStatement(sqlPrecioCombustible); st.setString(1, tipoCombustible); rs = st.executeQuery();
            if (!rs.next()) { throw new SQLException("Error crítico: No se pudo obtener precio del combustible " + tipoCombustible); }
            BigDecimal precioPorLitro = rs.getBigDecimal("precio_por_litro");
            rs.close(); st.close();

            BigDecimal diasBigDecimal = new BigDecimal(diasReservaOriginal);
            BigDecimal costoAlquiler = precioPorDia.multiply(diasBigDecimal);
            BigDecimal capacidadBigDecimal = new BigDecimal(capacidadDeposito);
            BigDecimal costoLlenarDeposito = precioPorLitro.multiply(capacidadBigDecimal);
            BigDecimal importeTotalFacturaEsperado = costoAlquiler.add(costoLlenarDeposito);

            String sqlFindFactura = "SELECT nroFactura FROM Facturas WHERE cliente = ? AND importe = ?";
            st = con.prepareStatement(sqlFindFactura); st.setString(1, dbNifCliente); st.setBigDecimal(2, importeTotalFacturaEsperado); rs = st.executeQuery();
            
            int nroFacturaParaEliminar = -1;
            if (rs.next()) {
                nroFacturaParaEliminar = rs.getInt("nroFactura");
                if (rs.next()) {
                    if(rs!=null) rs.close(); if(st!=null) st.close();
                    throw new SQLException("Ambigüedad: Múltiples facturas coinciden con los criterios para la anulación.");
                }
            }
            rs.close(); st.close();

            if (nroFacturaParaEliminar != -1) {
                LOGGER.info("Factura Nro {} encontrada para anular.", nroFacturaParaEliminar);
                
                String sqlDeleteLineas = "DELETE FROM Lineas_Factura WHERE NroFactura = ?";
                st = con.prepareStatement(sqlDeleteLineas);
                st.setInt(1, nroFacturaParaEliminar);
                st.executeUpdate();
                st.close();

                String sqlDeleteFactura = "DELETE FROM Facturas WHERE NroFactura = ?";
                st = con.prepareStatement(sqlDeleteFactura);
                st.setInt(1, nroFacturaParaEliminar);
                if (st.executeUpdate() == 1) {
                    LOGGER.info("Factura Nro {} eliminada con éxito.", nroFacturaParaEliminar);
                } else {
                    if(st!=null) st.close();
                    throw new SQLException("Error al intentar eliminar la factura Nro " + nroFacturaParaEliminar);
                }
                st.close();
            } else {
                LOGGER.warn("No se encontró una factura para la reserva ID {}. Se anulará la reserva sin eliminar factura.", idReserva);
            }

            // 7. Eliminar la reserva
            String sqlDeleteReserva = "DELETE FROM Reservas WHERE idReserva = ?";
            st = con.prepareStatement(sqlDeleteReserva);
            st.setInt(1, idReserva);
            int reservaDeleted = st.executeUpdate();
            if (reservaDeleted != 1) { 
                if(st!=null) st.close();
                throw new SQLException("Error inesperado al intentar eliminar la reserva ID: " + idReserva);
            }
            st.close();

            con.commit(); 
            LOGGER.info("Transacción de anulación confirmada para reserva ID {}.", idReserva);

        } catch (AlquilerCochesException ace) {
            LOGGER.warn("AlquilerCochesException en anulación (Reserva ID {}): {} (Código: {})", idReservaStr, ace.getMessage(), ace.getErrorCode());
            if (con != null) {
                try { 
                    con.rollback(); 
                } catch (SQLException exRollback) { 
                    LOGGER.error("Error CRÍTICO al intentar rollback tras AlquilerCochesException.", exRollback); 
                }
            }
            throw ace; 
        } catch (SQLException e) {
            LOGGER.error("SQLException en anulación (Reserva ID {}): {} (Código SQL: {})", idReservaStr, e.getMessage(), e.getErrorCode(), e);
            if (con != null) {
                try { 
                    con.rollback(); 
                } catch (SQLException exRollback) { 
                    LOGGER.error("Error CRÍTICO al intentar rollback tras SQLException.", exRollback); 
                }
            }
            throw e; 
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException e) { LOGGER.warn("Error cerrando ResultSet en anular_alquiler", e); }
            try { if (st != null) st.close(); } catch (SQLException e) { LOGGER.warn("Error cerrando PreparedStatement en anular_alquiler", e); }
            try { 
                if (con != null) {
                    con.close(); 
                }
            } catch (SQLException e) { LOGGER.warn("Error cerrando Connection en anular_alquiler", e); }
        }
	}
}
