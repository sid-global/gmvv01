from bottle import hook, response, route, run, static_file, request
import sqlite3
import psycopg2
import json
import socket
import ast
import time
import datetime
import decimal
import operator

tokenSistema = "sid1029"

# Maquina SID-DESARROLLO-01
#directorio = "C:/SID/En proceso/PDVSA/GMVV/"
#confBaseDatos = "dbname=cor_processu_ user=postgres password=root host=sid_db_02"

# Servidor Cirrus
directorio = "C:/SID/En proceso/PDVSA/GMVV/bottlepy"
confBaseDatos = "dbname=pdvsa_gmvv user=postgres password=Master1029"

# OJO: esta funcion evita que ocurran errores de tipo 'Access-Control-Allow-Origin'
# referidos a la conexion. Si estos errores ocurren ya son por otras razones.
@hook('after_request')
def enable_cors():
	response.headers['Access-Control-Allow-Origin'] = '*'

# Protocolo general: para acceder a cualquiera de estos servicios, el primer
# parametro enviado por GET debe ser el 'Token de Sistema', el cual se define 
# como una variable global. Esto evita que personas ajenas alteren la base de 
# datos desde la barra de direcciones de un navegador.

# *** Funcion maestra para consultas ***
# TK tiene el string con el token de sistema enviada por el cliente
# consulta tiene el string SQL que se ejecutara en la BD
# campos es una lista con los strings de los nombres de los campos que se van a obtener
# params es una lista con los strings de los parametros de los que depende la consulta SQL

#Modificada 17/09/2012 Alvaro
def consultas(TK, consulta, campos, params, modo):
	if TK == tokenSistema:
		conexion = psycopg2.connect(confBaseDatos)
		miCursor = conexion.cursor()
		nParams = len(params)
		if nParams == 0:
			try:
				miCursor.execute(consulta)
			except psycopg2.IntegrityError:
				return json.dumps({"estado":"fracaso"}, ensure_ascii=False)
		elif nParams > 0:
			try:
				miCursor.execute(consulta,tuple(params))
			except psycopg2.IntegrityError:
				return json.dumps({"estado":"fracaso"}, ensure_ascii=False)
		if modo == "consulta":
			losDatos = miCursor.fetchall()
			resultado = []
			nCampos = len(campos)
			for tupla in losDatos:
				i = 0
				elDicc = "{"
				while i < nCampos:
					if isinstance(tupla[i], int) or isinstance(tupla[i], long) or isinstance(tupla[i], decimal.Decimal):
						elDicc += "'" + campos[i] + "':" + str(tupla[i]) + ","
					elif isinstance(tupla[i], str) or isinstance(tupla[i], datetime.datetime):
						elDicc += "'" + campos[i] + "':'" + unicode(str(tupla[i]), "utf-8") + "',"
					else:
						elDicc += "'" + campos[i] + "':'" + unicode(str(tupla[i]), "utf-8") + "',"
					i+=1
				elDicc = elDicc[:len(elDicc)-1]
				elDicc += "}"
				try:
					print "\n" + elDicc + "\n"
				except UnicodeEncodeError:
					print "\nError de Unicode\n"
				resultado.append(ast.literal_eval(elDicc))
			return json.dumps(resultado, ensure_ascii=False)
		elif modo == "inserta" or  modo == "elimina" or modo == "modifica":
			conexion.commit()
			return json.dumps({"estado":"exito","sec":params[0]}, ensure_ascii=False)
		conexion.close()
	else:
		return json.dumps({"estado":""}, ensure_ascii=False)




#############################################################################################################################
####                                               TABLA cor_seg_users                                                   ####
#############################################################################################################################
@route('/t_cor_seg_users_0001_consulta_login')
def t_cor_seg_users_0001_consulta_login():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "select name, active from cor_seg_users where login = %s and pswd = %s"
	campos = ["name","active"]
	parametros = [recibido["login"],recibido["pswd"]]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	if resultado != "":
		if len(resultado) == 1:
			if resultado[0]["active"] == "S":
				return json.dumps({"name":resultado[0]["name"]})
			else:
				return "inactivo"
		else:
			return "fracaso"

#############################################################################################################################
####                                          TABLA cor_inv_movimiento_lote                                              ####
#############################################################################################################################
@route('/t_cor_inv_movimiento_lote_0001_consulta_codificacion')	
def CCodificacionAll():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT a.cor_movimiento_sec, b.cor_responsable_nombre, a.cor_movimiento_fecha, a.cor_movimiento_id
				   FROM cor_inv_movimiento_lote a JOIN cor_crm_responsable b ON  a.cor_responsable_sec = b.cor_responsable_sec
				   WHERE cor_movimiento_tipo='C' and cor_movimiento_estatus = 'A' ORDER BY a.cor_movimiento_sec DESC;"""
	campos = ["sec","resp","fecha","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_lote_0002_consulta_codificacion')
def CCodificacion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	FI = recibido["FI"]
	FF = recibido["FF"]
	query = """SELECT a.cor_movimiento_sec, b.cor_responsable_nombre, a.cor_movimiento_fecha, a.cor_movimiento_id
				   FROM cor_inv_movimiento_lote a JOIN cor_crm_responsable b ON  a.cor_responsable_sec = b.cor_responsable_sec
				   WHERE cor_movimiento_tipo='C' and a.cor_movimiento_fecha between '"""+FI+" 00:00:00' and '"+FF+""" 23:59:59' ORDER BY a.cor_movimiento_sec DESC;"""
	campos = ["sec","resp","fecha","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_0003_insertar_codificacion')
def IngresarCod():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	fechah=recibido["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	query = """ INSERT INTO cor_inv_movimiento_lote(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_movimiento_tipo, cor_responsable_sec, 
            cor_movimiento_fec_mod,cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_estatus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'A');"""
	campos = []
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_lote_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fechah,"C",recibido["resp"],datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"),str(socket.gethostbyname(socket.gethostname())),recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")

@route('/t_cor_inv_movimiento_lote_0004_consultar_matcat_codificacion')
def PreCodif():	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ SELECT SUM(cor_movlote_por_codificar), det.cor_matcat_sec, matcat.cor_matcat_id
				FROM cor_inv_matcat matcat,cor_inv_movimiento_lote_detalle det,cor_inv_movimiento_lote lote
				WHERE lote.cor_movimiento_sec=det.cor_movimiento_sec and lote.cor_movimiento_estatus='P' and 
				det.cor_movlote_por_codificar > 0 and det.cor_matcat_sec = matcat.cor_matcat_sec and 
				lote.cor_movimiento_tipo='R'
				GROUP BY  det.cor_matcat_sec, matcat.cor_matcat_id
				ORDER BY matcat.cor_matcat_id DESC;"""
	campos = ["sum","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_0005_insertar_inventario_inicial')
def t_cor_inv_movimiento_lote_0005_insertar_inventario_inicial():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_movimiento_lote(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_movimiento_tipo, cor_responsable_sec, cor_movimiento_estatus, 
            cor_movimiento_fec_mod,cor_movimiento_ip_mod, cor_movimiento_login_mod,cor_almacen_sec)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fecha = recibido["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	ip_mod = str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_lote_sec');")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["des"],fecha,"R",recibido["resp"],"S",datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"),ip_mod,recibido["user"],recibido["alm"]]
	return consultas(TK, query, campos, parametros,"inserta")

@route('/t_cor_inv_movimiento_lote_0006_consulta_inventario_inicial')
def t_cor_inv_movimiento_lote_0006_consulta_inventario_inicial():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT a.cor_movimiento_sec, a.cor_movimiento_id, b.cor_almacen_id
				   FROM cor_inv_movimiento_lote a join cor_inv_almacen b ON a.cor_movimiento_ip_mod = b.cor_almacen_sec
				   WHERE a.cor_movimiento_sec = %s ORDER BY a.cor_movimiento_sec DESC;"""
	campos = ["sec","id","alm"]
	parametros = [recibido["mod"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_lote_0007_consulta_inventario_inicia')
def t_cor_inv_movimiento_lote_0007_consulta_inventario_inicia():	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	sec = recibido["sec"]
	query = """SELECT a.cor_movimiento_sec, a.cor_movimiento_id, a.cor_movimiento_fecha, a.cor_movimiento_estatus, b.cor_almacen_id, c.cor_responsable_nombre
				   FROM cor_inv_movimiento_lote a join cor_inv_almacen b ON a.cor_almacen_sec = b.cor_almacen_sec JOIN cor_crm_responsable c ON a.cor_responsable_sec = c.cor_responsable_sec
				   WHERE a.cor_movimiento_tipo='R' and a.cor_movimiento_estatus != 'P' and a.cor_movimiento_fecha between '"""+recibido["FI"]+" 00:00:00' and '"+recibido["FF"]+" 23:59:59' and a.cor_factura_sec is null """
	campos = ["sec","id","fecha","estatus","alm","resp"]
	
	if( int(sec) > 0 ):
		query+="and lote.cor_factura_sec= %s ORDER BY a.cor_movimiento_sec DESC"
		parametros = [sec]
	else:
		query+="ORDER BY a.cor_movimiento_sec DESC"
		parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_0008_consulta_inventario_inicia')
def t_cor_inv_movimiento_lote_0008_consulta_inventario_inicia():	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT a.cor_movimiento_sec, a.cor_movimiento_id, a.cor_movimiento_fecha, a.cor_movimiento_estatus, b.cor_almacen_id, c.cor_responsable_nombre
				   FROM cor_inv_movimiento_lote a join cor_inv_almacen b ON a.cor_almacen_sec = b.cor_almacen_sec JOIN cor_crm_responsable c ON a.cor_responsable_sec = c.cor_responsable_sec
				   WHERE a.cor_movimiento_sec = %s ORDER BY a.cor_movimiento_sec DESC"""
	campos = ["sec","id","fecha","estatus","alm","resp"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_0009_consulta_inventario_inicial')
def t_cor_inv_movimiento_lote_0009_consulta_inventario_inicial():	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT a.cor_movimiento_sec, a.cor_movimiento_id, a.cor_movimiento_fecha, a.cor_movimiento_estatus, b.cor_almacen_id, c.cor_responsable_nombre
				   FROM cor_inv_movimiento_lote a join cor_inv_almacen b ON a.cor_almacen_sec = b.cor_almacen_sec JOIN cor_crm_responsable c ON a.cor_responsable_sec = c.cor_responsable_sec
				   WHERE a.cor_movimiento_tipo='R' and a.cor_movimiento_estatus != 'P' and a.cor_factura_sec is null ORDER BY a.cor_movimiento_sec DESC"""
	campos = ["sec","id","fecha","estatus","alm","resp"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_lote_0010_modifica_inventario_inicial')
def t_cor_inv_movimiento_lote_0010_modifica_inventario_inicial():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	query = """UPDATE cor_inv_movimiento_lote
				   SET cor_movimiento_estatus='P', cor_movimiento_fec_mod= %s, cor_movimiento_ip_mod= %s, cor_movimiento_login_mod= %s
				   WHERE cor_movimiento_sec = %s;"""
	campos = []
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")

@route('/t_cor_inv_movimiento_lote_0011_consulta_movimientos')
def t_cor_inv_movimiento_lote_0011_consulta_despachos():	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT mov.cor_movimiento_sec, mov.cor_responsable_sec, mov.cor_movimiento_fecha,mov.cor_movimiento_id, resp.cor_responsable_nombre
			    FROM cor_inv_movimiento mov JOIN cor_crm_responsable resp ON mov.cor_responsable_sec = resp.cor_responsable_sec
			    WHERE cor_movimiento_tipo= %s and cor_movimiento_estatus != 'C' and cor_movimiento_estatus != 'P'"""
	campos = ["sec","resp","fecha","id","nombre"]
	if (recibido["FI"] == ""):
		query += " ORDER BY mov.cor_movimiento_sec DESC;"
		parametros = [recibido["tipo"]]
		print query
		return consultas(TK, query, campos, parametros,"consulta")
	else:
		fi = recibido["FI"]+ " 00:00:00"
		ff = recibido["FF"]+ " 23:59:59"
		query += " and cor_movimiento_fecha between %s and %s ORDER BY mov.cor_movimiento_sec DESC;"
		parametros = [recibido["tipo"],fi,ff]
		return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_0011_consulta_lotes_por_codificar')
def ConsultaLxC(almacen):
	myReturnData=[]
	
	cur.execute("""SELECT lote_det.cor_matcat_sec, lote_det.cor_matcat_n1_sec,lote_det.cor_matcat_n2_sec,lote_det.cor_matcat_n3_sec, lote.cor_movimiento_id, lote.cor_movimiento_fecha, lote_det.cor_movlote_por_codificar  
				FROM cor_inv_movimiento_lote lote,cor_inv_movimiento_lote_detalle lote_det
				WHERE lote_det.cor_almacen_sec="""+almacen+" and lote.cor_movimiento_sec=lote_det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus='P' ;")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result2=cur.fetchone()
			cur.execute("SELECT cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec="+str(row[1])+";")
			result3=cur.fetchone()
			cur.execute("SELECT cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+";")
			result4=cur.fetchone()
			cur.execute("SELECT cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result5=cur.fetchone()	
			myReturnData.append({"matcat":result2[0],"matcat_n1":result3[0],"matcat_n2":result4[0],"matcat_n3":result5[0],"recep":row[4],"fecha":str(row[5]),"nro":row[6]})

	return json.dumps(myReturnData)
	
@route('/t_cor_inv_movimiento_lote_0012_consulta_codificacion')
def ConsultaCodif():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec,resp.cor_responsable_nombre,SUM(cor_movlote_cantidad)
				   FROM cor_inv_movimiento_lote as lote JOIN cor_crm_responsable resp ON lote.cor_responsable_sec = resp.cor_responsable_sec lEFT JOIN cor_inv_movimiento_lote_detalle det ON lote.cor_movimiento_sec = det.cor_movimiento_sec
				   WHERE lote.cor_movimiento_sec = %s
				   Group by lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec,resp.cor_responsable_nombre ORDER BY lote.cor_movimiento_sec"""
	campos = ["id","desc","fecha","rsec","resp","sum"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_lote_0013_modifica_cierra_codificacion')
def t_cor_inv_movimiento_lote_0013_modifica_cierra_codificacion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	query = """UPDATE cor_inv_movimiento_lote
				   SET cor_movimiento_estatus='C', cor_movimiento_fec_mod= '"""+fech_mod+"""', cor_movimiento_ip_mod= '"""+ip_mod+"""', cor_movimiento_login_mod= '"""+recibido["user"]+"""'
				   WHERE cor_movimiento_sec = """+recibido["sec"]+""";"""
	print query
	campos = []
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
#############################################################################################################################
####                                      TABLA cor_inv_movimiento_lote_detalle                                          ####
#############################################################################################################################
@route('/t_cor_inv_movimiento_lote_detalle_0001_consulta_inventario_inicial')
def t_cor_inv_movimiento_lote_detalle_0001_consulta_inventario_inicial():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT a.cor_matcat_sec, b.cor_matcat_id, a.cor_movlote_cantidad
				   FROM cor_inv_movimiento_lote_detalle a JOIN cor_inv_matcat b ON a.cor_matcat_sec = b.cor_matcat_sec
				   WHERE a.cor_movimiento_sec = %s ORDER BY b.cor_matcat_id"""
	campos = ["sec","id","cant"]
	parametros = [recibido["mov"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_detalle_0001_consultar_detalle_codificacion')
def ListDetCod():
	recibido = dict(request.GET)
	myReturnData=[]
	TK = recibido["TK"]
	query = """SELECT matcat.cor_matcat_id,matcat.cor_matcat_sec,sum(det.cor_movlote_cantidad) as cant
				   FROM cor_inv_movimiento_lote_detalle det, cor_inv_matcat matcat
				   WHERE det.cor_movimiento_sec = %s and det.cor_matcat_sec=matcat.cor_matcat_sec  
				   GROUP BY matcat.cor_matcat_id,matcat.cor_matcat_sec ORDER BY matcat.cor_matcat_id;"""
	campos = ["id","sec","cant"]
	parametros = [recibido["mov"]]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	for result in resultado:
		query2 = """SELECT sum(cor_movlote_por_codificar) as por_cod
				   FROM cor_inv_movimiento_lote_detalle det,cor_inv_movimiento_lote lote
				   WHERE lote.cor_movimiento_sec=det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and
				   det.cor_matcat_sec = %s; """
		campos2 = ["por_cod"]
		parametros2 = [result["sec"]]
		resultadoSum = eval(consultas(TK, query2, campos2, parametros2, "consulta")+"")
		myReturnData.append({"material":result["id"],"cod":result["sec"],"por":result[0]["por_cod"]})
		
	return json.dumps(myReturnData)
	
@route('/t_cor_inv_movimiento_lote_detalle_0002_consultar_detalle_codificacion')
def PreCodifMatcat():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT sum(det.cor_movlote_por_codificar) 
				   FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
				   WHERE det.cor_matcat_sec = %s and det.cor_movimiento_sec=lote.cor_movimiento_sec and 
				   lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus='P';"""
	campos = ["pend"]
	parametros = [recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_lote_detalle_0002_insertar_inventario_detalles')
def t_cor_inv_movimiento_lote_detalle_0002_insertar_inventario_detalles():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	fecha = str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod = str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_lote_detalle_sec');")
	losDatos = miCursor.fetchall()
	query = """ INSERT INTO cor_inv_movimiento_lote_detalle(
            cor_movlote_sec, cor_movimiento_sec, cor_matcat_sec, cor_movlote_cantidad, 
            cor_movlote_fec_mod, cor_movlote_ip_mod, cor_movlote_login_mod, cor_movlote_codificados, cor_movlote_por_codificar, cor_movlote_tipo)
    VALUES ("""+str(losDatos[0][0])+", "+str(recibido["mov"])+", "+str(recibido["matcat"])+", "+str(recibido["cant"])+", '"+fecha+"', '"+ip_mod+"', '"+str(recibido["user"])+"', 0, "+str(recibido["cant"])+", 'R');"""
	print query
	campos = []
	parametros = []
	return consultas(TK, query, campos, parametros,"inserta")

@route('/t_cor_inv_movimiento_lote_detalle_0005_consultar_detalle_codificacion')
def t_cor_inv_movimiento_lote_detalle_0005_consultar_detalle_codificacion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT matcat.cor_matcat_id, mov.cor_movlote_cantidad, mov.cor_movlote_por_codificar, cor_movlote_codificados
			FROM cor_inv_movimiento_lote_detalle mov JOIN cor_inv_matcat matcat on mov.cor_matcat_sec = matcat.cor_matcat_sec
			WHERE mov.cor_movimiento_sec = %s AND mov.cor_movlote_cantidad > 0 ORDER BY matcat.cor_matcat_id;"""
	campos = ["id","cant","xcod","codif"]
	parametros = [recibido["mov"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_movimiento_lote_detalle_0006_consultar_por_codificar')
def t_cor_inv_movimiento_lote_detalle_0006_consultar_por_codificar():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """
SELECT sum(det.cor_movlote_por_codificar)
	       FROM cor_inv_matcat matcat,cor_inv_movimiento_lote_detalle det,cor_inv_movimiento_lote lote
	       WHERE lote.cor_movimiento_sec=det.cor_movimiento_sec and lote.cor_movimiento_estatus='P' and 
	       det.cor_movlote_por_codificar > 0 and det.cor_matcat_sec = matcat.cor_matcat_sec and 
		   lote.cor_movimiento_tipo='R' and det.cor_matcat_sec = %s;"""
	campos = ["xcod"]
	parametros = [recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
#New 17/09/2012 Alvaro
#Modificado 18/09/2012 MT
@route('/t_cor_inv_movimiento_lote_detalle_0007_generar_codificacion')
def t_cor_inv_movimiento_lote_detalle_0007_generar_codificacion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	cant= int(recibido["CF"])-int(recibido["CI"])+1
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	query = "SELECT cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec FROM cor_inv_matcat WHERE cor_matcat_sec=%s;"
	campos = ["matcat_n1","matcat_n2","matcat_n3"]
	parametros = [recibido["matcat"]]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	
	query="""INSERT INTO cor_inv_articulo(
            cor_articulo_sec,cor_articulo_id,cor_articulo_desc,cor_articulo_serial,cor_almacen_sec, cor_matcat_sec, cor_matcat_n3_sec, cor_matcat_n2_sec, cor_matcat_n1_sec, 
			cor_articulo_compuesto, cor_articulo_fec_mod, cor_articulo_ip_mod, cor_articulo_login_mod, cor_articulo_estatus, cor_color_sec, cor_matprima_sec)
			VALUES (%s,'','','',%s,%s,%s,%s,%s,'N',%s,%s,%s,138,%s,%s);"""
	campos = []	
	
	if resultado != "":
		
		i=int(recibido["CI"]);
		while i<=int(recibido["CF"]):
			#Inserta Articulos
			parametros = [i,recibido["almacen"],recibido["matcat"],resultado[0]["matcat_n3"],resultado[0]["matcat_n2"],resultado[0]["matcat_n1"],fech_mod,ip_mod,recibido["user"],recibido["color"],recibido["matprima"]]
			eval(consultas(TK, query, campos, parametros,"inserta")+"")
			i+=1
		
		#Inserta detalle de la Codificacion
		#movlote tipo error
		query="""INSERT INTO cor_inv_movimiento_lote_detalle(
            cor_movlote_sec, cor_matcat_sec, cor_matcat_n3_sec, cor_matcat_n2_sec, 
            cor_matcat_n1_sec, cor_movimiento_sec, cor_almacen_sec, cor_movlote_cantidad, cor_movlote_tipo,
            cor_movlote_fec_mod, cor_movlote_ip_mod, cor_movlote_login_mod, 
            cor_movlote_campo1, cor_movlote_campo2)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'',%s,%s,%s,%s,%s);"""		
		
		
		conexion = psycopg2.connect(confBaseDatos)
		miCursor = conexion.cursor()
		miCursor.execute("SELECT nextval('cor_inv_movimiento_lote_detalle_sec');")
		losDatos = miCursor.fetchall()
		miCursor.close()
		
		parametros = [losDatos[0][0],recibido["matcat"],resultado[0]["matcat_n3"],resultado[0]["matcat_n2"],resultado[0]["matcat_n1"],recibido["mov"],recibido["almacen"],cant,fech_mod,ip_mod,recibido["user"],recibido["CI"],recibido["CF"]]
		eval(consultas(TK, query, campos, parametros,"inserta")+"")
		
		#Actualiza detalle de las recepciones
		
		query="""SELECT det.cor_movlote_sec,det.cor_movlote_codificados, det.cor_movlote_por_codificar,det.cor_movlote_cantidad
				   FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
				   WHERE det.cor_movimiento_sec=lote.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and
				   det.cor_matcat_sec=%s and lote.cor_movimiento_estatus='P' and det.cor_movlote_por_codificar > 0;"""
		campos = ["movlote_sec","codificados","xcod","cant"]
		parametros = [recibido["matcat"]]
		resultado1 = eval(consultas(TK, query, campos, parametros,"consulta")+"")
		
		if resultado1 != "":
			
			i=int(recibido["CI"])
			ii=0
			while cant>0:
				if resultado1[ii]["xcod"]<cant: 
					cod=resultado1[ii]["cant"]
					xcod=0
					cant-=resultado1[ii]["xcod"]
					
				else:
					cod=resultado1[ii]["codificados"]+cant
					xcod=resultado1[ii]["xcod"]-cant
					cant=0
		
				query="""UPDATE cor_inv_movimiento_lote_detalle
				SET cor_almacen_sec=%s, cor_movlote_fec_mod=%s, cor_movlote_ip_mod=%s, 
				cor_movlote_login_mod=%s, cor_movlote_codificados=%s, cor_movlote_por_codificar=%s
				WHERE cor_movlote_sec=%s;"""
				campos=[]
				parametros=[recibido["almacen"],fech_mod,ip_mod,recibido["user"],cod,xcod,resultado1[ii]["movlote_sec"]]
				eval(consultas(TK, query, campos, parametros,"inserta")+"")
				i+=resultado1[ii]["xcod"]
				ii+=1

@route('/t_cor_inv_movimiento_lote_detalle_0008_consultar_lotes_por_codificar')
def t_cor_inv_movimiento_lote_detalle_0008_consultar_lotes_por_codificar():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT SUM(det.cor_movlote_por_codificar), det.cor_matcat_sec, mov.cor_almacen_sec, alm.cor_almacen_id, mat.cor_matcat_id, mov.cor_movimiento_tipo
				FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote mov, cor_inv_almacen alm, cor_inv_matcat mat
				WHERE det.cor_matcat_sec = mat.cor_matcat_sec AND mov.cor_almacen_sec = alm.cor_almacen_sec AND det.cor_movimiento_sec = mov.cor_movimiento_sec AND mov.cor_movimiento_tipo = 'R' AND det.cor_movlote_por_codificar > 0"""
	campos = ["sum","matcat_sec","alm_sec","alm_id","matcat_id","tipo"]	
	if (recibido["matcat"] == "") :
		query += " GROUP BY det.cor_matcat_sec, mov.cor_almacen_sec, alm.cor_almacen_id, mat.cor_matcat_id, mov.cor_movimiento_tipo ORDER BY mat.cor_matcat_id;"
		parametros = []
		return consultas(TK, query, campos, parametros,"consulta")
	else:
		query += " and det.cor_matcat_sec = %s GROUP BY det.cor_matcat_sec, mov.cor_almacen_sec, alm.cor_almacen_id, mat.cor_matcat_id, mov.cor_movimiento_tipo ORDER BY mat.cor_matcat_id;"
		parametros = [recibido["matcat"]]
		return consultas(TK, query, campos, parametros,"consulta")
	campos = ["xcod"]
	parametros = [recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"consulta")				
#############################################################################################################################
####                                                   TABLA cor_inv_matcat                                              ####
#############################################################################################################################
@route('/t_cor_inv_matcat_0001_consulta_matcat')
def t_cor_inv_matcat_0001_consulta_matcat():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_sec, cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_activo = 'S' ORDER BY cor_matcat_id"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
#############################################################################################################################
####                                                  TABLA cor_inv_almacen                                              ####
#############################################################################################################################
@route('/t_cor_inv_almacen_0001_consulta_almacen')
def CAlmacen():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT alm.cor_almacen_sec, alm.cor_almacen_id, res.cor_responsable_nombre
				   FROM cor_inv_almacen alm,cor_crm_responsable res
				   WHERE alm.cor_responsable_sec=res.cor_responsable_sec ORDER BY alm.cor_almacen_id;"""
	campos = ["sec","id","resp"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_almacen_0002_consulta_almacen_activo')
def t_cor_inv_almacen_0002_consulta_almacen_activo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_almacen_sec,cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_activo='S' ORDER BY cor_almacen_id;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
## 17/09/2012 Alvaro
@route('/t_cor_inv_almacen_0003_modifica_almacen')
def ActAlm():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """UPDATE cor_inv_almacen
				   SET cor_almacen_tipo = %s, cor_responsable_sec = %s, cor_almacen_activo = %s,
					    cor_almacen_fec_mod = %s, cor_almacen_ip_mod = %s, 
					   cor_almacen_login_mod = %s
				   WHERE cor_almacen_sec = %s"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	parametros = [recibido["tipo"],recibido["resp_sec"],recibido["activo"],fech_mod,ip_mod,recibido["user"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
# 17/09/2012 Alvaro
@route('/t_cor_inv_almacen_0004_insertar_almacen')
def NewAlmacen():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query="""INSERT INTO cor_inv_almacen(
            cor_almacen_sec, cor_almacen_id, cor_almacen_desc, cor_almacen_tipo, 
            cor_responsable_sec, cor_almacen_activo, cor_almacen_fec_mod, 
            cor_almacen_ip_mod, cor_almacen_login_mod)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	tiempo = time.localtime()
	ano = str(tiempo[0])
	mes = str(tiempo[1])
	dia = str(tiempo[2])
	hora = str(tiempo[3])
	minutos = str(tiempo[4])
	segundos = str(tiempo[5])
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("select nextval('cor_inv_almacen_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],recibido["tipo"],recibido["resp_sec"],recibido["activo"],ano+"-"+mes+"-"+dia+" "+hora+":"+minutos+":"+segundos+"-4:30",ip_mod,recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")	

@route('/t_cor_inv_almacen_0005_consulta_detalle_almacen')
def CAlmacenaux():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT alm.cor_almacen_id,alm.cor_almacen_desc,alm.cor_almacen_tipo,alm.cor_responsable_sec,alm.cor_almacen_activo
				   FROM cor_inv_almacen alm
				   WHERE alm.cor_almacen_sec=%s ORDER BY alm.cor_almacen_id;"""
	campos = ["id","desc","tipo","resp","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")	

#############################################################################################################################
####                                                 TABLA cor_inv_articulo                                              ####
#############################################################################################################################
@route('/t_cor_inv_articulo_0001_consulta_verificar_codigos')
def t_cor_inv_articulo_0001_consulta_verificar_codigos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_articulo_sec FROM cor_inv_articulo WHERE cor_articulo_sec BETWEEN %s AND %s;"
	campos = ["sec"]
	parametros = [recibido["CI"],recibido["CF"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_articulo_0002_consulta_validacion_despacho')
def t_cor_inv_articulo_0002_consulta_validacion_despacho():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	CI = int(recibido["CI"])
	CF = recibido["CF"]
	if (CF == ""):
		CF = int(recibido["CI"])
	else:
		CF = int(recibido["CF"])
	codigos = '('
	i = CI;
	while i <= CF:
		c = str(i)
		if (codigos == '(') :
			codigos = codigos + c
		else:
			codigos = codigos + ', ' + str(i)
		i+=1
	codigos = codigos +')'
	query = """	SELECT cor_articulo_sec
				FROM cor_inv_articulo
				WHERE cor_articulo_sec IN """+codigos+""" AND cor_almacen_sec = %s 
				AND cor_matcat_sec IN (SELECT cor_matcat_sec FROM cor_inv_composicion_detalle WHERE cor_composicion_sec = %s) AND cor_articulo_estatus = 138;"""
	campos = ["sec"]
	parametros = [recibido["alm"],recibido["comp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_articulo_0003_consulta_articulos_no_desp')
def t_cor_inv_articulo_0003_consulta_articulos_no_desp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT art.cor_articulo_sec, art.cor_matcat_sec, art.cor_articulo_estatus, art.cor_almacen_sec, art.cor_articulo_compuesto, matcat.cor_matcat_id, est.cor_estatus_avance_id, alm.cor_almacen_id
				FROM cor_inv_articulo art JOIN cor_inv_matcat matcat ON art.cor_matcat_sec = matcat.cor_matcat_sec
					JOIN cor_crm_estatus_avance est ON art.cor_articulo_estatus = est.cor_estatus_avancesec
					JOIN cor_inv_almacen alm ON art.cor_almacen_sec = alm.cor_almacen_sec
				WHERE cor_articulo_estatus != 139 ORDER BY matcat.cor_matcat_id;"""
	campos = ["sec","matcat_sec","est_sec","alm_sec","compuesto","matcat","estatus","almacen"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_articulo_0004_consulta_articulo')
def t_cor_inv_articulo_0004_consulta_articulo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT art.cor_articulo_sec, art.cor_matcat_sec, art.cor_articulo_estatus, art.cor_almacen_sec, art.cor_articulo_compuesto, matcat.cor_matcat_id, est.cor_estatus_avance_id, alm.cor_almacen_id
				FROM cor_inv_articulo art JOIN cor_inv_matcat matcat ON art.cor_matcat_sec = matcat.cor_matcat_sec
					JOIN cor_crm_estatus_avance est ON art.cor_articulo_estatus = est.cor_estatus_avancesec
					JOIN cor_inv_almacen alm ON art.cor_almacen_sec = alm.cor_almacen_sec
				WHERE cor_articulo_estatus != 139"""
	campos = ["sec","matcat_sec","est_sec","alm_sec","compuesto","matcat","estatus","almacen"]
	if (str(recibido["codigo"]) != "" and str(recibido["tipo"]) != ""):
		query += ' and art.cor_articulo_sec = %s and art.cor_matcat_sec = %s ORDER BY art.cor_articulo_sec'
		parametros = [recibido["codigo"],recibido["tipo"]]
		return consultas(TK, query, campos, parametros,"consulta")
	elif (str(recibido["codigo"]) != ""):
		query += ' and art.cor_articulo_sec = %s ORDER BY art.cor_articulo_sec'
		parametros = [recibido["codigo"]]
		return consultas(TK, query, campos, parametros,"consulta")
	elif (str(recibido["tipo"]) != ""):
		query += ' and art.cor_matcat_sec = %s ORDER BY art.cor_articulo_sec'
		parametros = [recibido["tipo"]]
		return consultas(TK, query, campos, parametros,"consulta")
	else:
		query += ' ORDER BY art.cor_articulo_sec';
		parametros = []
		return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_articulo_0005_consulta_articulo_detalles')
def t_cor_inv_articulo_0005_consulta_articulo_detalles():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT art.cor_articulo_desc, art.cor_articulo_serial, art.cor_color_sec, art.cor_matprima_sec, art.cor_matcat_sec, art.cor_articulo_estatus, art.cor_almacen_sec, art.cor_articulo_compuesto, matcat.cor_matcat_id, est.cor_estatus_avance_id, alm.cor_almacen_id
				FROM cor_inv_articulo art JOIN cor_inv_matcat matcat ON art.cor_matcat_sec = matcat.cor_matcat_sec
					JOIN cor_crm_estatus_avance est ON art.cor_articulo_estatus = est.cor_estatus_avancesec
					JOIN cor_inv_almacen alm ON art.cor_almacen_sec = alm.cor_almacen_sec
				WHERE cor_articulo_sec = %s"""
	campos = ["desc","serial","color","mat_prima","matcat_sec","est_sec","alm_sec","compuesto","matcat","estatus","almacen"]	
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_articulo_0005_actualiza_articulo')	
def t_cor_inv_articulo_0005_actualiza_articulo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_articulo
				SET cor_articulo_fec_mod = %s, cor_articulo_ip_mod = %s, cor_articulo_login_mod = %s,
				cor_articulo_desc = %s, cor_articulo_serial = %s, cor_color_sec = %s, cor_matprima_sec = %s, cor_articulo_compuesto = %s WHERE cor_articulo_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["desc"],recibido["serial"],recibido["color"],recibido["matprima"],recibido["comp"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_articulo_0007_consulta_articulos_disponibles')
def t_cor_inv_articulo_0007_consulta_articulos_disponibles():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	alm = recibido["alm"]
	query = """	SELECT Count(*), art.cor_almacen_sec, alm.cor_almacen_id FROM cor_inv_articulo art, cor_inv_almacen alm WHERE art.cor_almacen_sec = alm.cor_almacen_sec and cor_matcat_sec = %s 
				and cor_articulo_estatus != 139"""
	campos = ["cant","alm_sec","alm_id"]
	if (alm == ""):
		query += " GROUP BY art.cor_almacen_sec, alm.cor_almacen_id;"
		parametros = [recibido["matcat"]]
		return consultas(TK, query, campos, parametros,"consulta")
	else:
		query += " and art.cor_almacen_sec = %s GROUP BY art.cor_almacen_sec, alm.cor_almacen_id;"
		parametros = [recibido["matcat"],alm]
		return consultas(TK, query, campos, parametros,"consulta")

#############################################################################################################################
####                                           TABLA cor_inv_articulo_componente                                         ####
#############################################################################################################################
@route('/t_cor_inv_articulo_componente_0001_consulta_articulo_componente')
def t_cor_inv_articulo_componente_0001_consulta_articulo_componente():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT a.cor_articulo_componente_sec, b.cor_matcat_sec, b.cor_articulo_estatus, b.cor_almacen_sec, c.cor_matcat_id, d.cor_estatus_avance_id, e.cor_almacen_id
				FROM cor_inv_articulo_componente a JOIN cor_inv_articulo b ON a.cor_articulo_componente_sec = b.cor_articulo_sec
					JOIN cor_inv_matcat c ON b.cor_matcat_sec = c.cor_matcat_sec
					JOIN cor_crm_estatus_avance d ON b.cor_articulo_estatus = d.cor_estatus_avancesec
					JOIN cor_inv_almacen e ON b.cor_almacen_sec = e.cor_almacen_sec
				WHERE a.cor_articulo_compuesto_sec = %s ORDER BY c.cor_matcat_id"""
	campos = ["sec","matcat_sec","est_sec","alm_sec","matcat","estatus","almacen"]	
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")	

@route('/t_cor_inv_articulo_componente__0002_inserta_componente')
def t_cor_inv_articulo_componente__0002_inserta_componente():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "INSERT INTO cor_inv_articulo_componente (cor_articulo_compuesto_sec, cor_articulo_componente_sec) VALUES ( %s, %s);"
	campos = []
	parametros = [recibido["comp"],recibido["compto"]]
	return consultas(TK, query, campos, parametros,"inserta")
#############################################################################################################################
####                                               TABLA cor_crm_responsable                                             ####
#############################################################################################################################	
@route('/t_cor_crm_responsable_0001_consulta_responsable')
def ConsultaResp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_responsable_sec,cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_activo='S' ORDER BY cor_responsable_nombre;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_responsable_0002_consulta_datos_responsable')
def CResponsable():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT res.cor_responsable_sec,res.cor_responsable_nombre,tipo.cor_tiporesponsable_id,tipo.cor_tiporesponsable_sec,res.cor_responsable_activo
				   FROM cor_crm_responsable res, cor_crm_tipo_responsable tipo
				   WHERE res.cor_tiporesponsable_sec=tipo.cor_tiporesponsable_sec ORDER BY cor_responsable_nombre;"""
	campos = ["sec","nombre","tipoid","tipo","activo"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_responsable_0003_insertar_responsable')
def CrearResp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_responsable(
            cor_responsable_sec, cor_responsable_nombre, cor_tiporesponsable_sec, 
            cor_responsable_activo, cor_responsable_fec_mod, cor_responsable_ip_mod, 
            cor_responsable_login_mod)
    VALUES (%s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	tiempo = time.localtime()
	ano = str(tiempo[0])
	mes = str(tiempo[1])
	dia = str(tiempo[2])
	hora = str(tiempo[3])
	minutos = str(tiempo[4])
	segundos = str(tiempo[5])
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("select nextval('cor_crm_responsable_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["nombre"],recibido["tipo"],recibido["activo"],ano+"-"+mes+"-"+dia+" "+hora+":"+minutos+":"+segundos+"-4:30",ip_mod,recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
@route('/t_cor_crm_responsable_0004_actualiza_responsable')
def ActResp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """UPDATE cor_crm_responsable
				   SET cor_responsable_nombre = %s, cor_tiporesponsable_sec = %s, 
					    cor_responsable_fec_mod = %s, cor_responsable_ip_mod = %s, 
					   cor_responsable_login_mod = %s
				   WHERE cor_responsable_sec = %s"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("select nextval('cor_crm_responsable_sec')")
	losDatos = miCursor.fetchall()
	parametros = [recibido["nom"],recibido["tipo"],fech_mod,ip_mod,recibido["user"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")



#############################################################################################################################
####                                               TABLA cor_inv_almacen_zona                                            ####
#############################################################################################################################		
@route('/t_cor_inv_almacen_zona_0001_consulta_ubicacion')
def ConsultaZona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	tipo = recibido["tipo"]
	alm = recibido["almacen"]
	if (tipo=='almacen'):
		consulta05='cor_inv_almacen_zona'
		consulta15='almacen.cor_almacen_sec='
		consulta04='cor_crm_tipo_zona tipo WHERE '
		consultal4='tipo.cor_tipo_zona_id, almacen.cor_almacen_zona_sec FROM '
		consulta004=' and almacen.cor_tipo_zona_sec = tipo.cor_tipo_zona_sec;'
	else:
		consulta05='cor_crm_responsable_zona'
		consulta15='almacen.cor_responsable_sec='
		consulta04='cor_crm_tipo_zona tipo WHERE '
		consultal4='tipo.cor_tipo_zona_id, almacen.cor_responsable_zona_sec FROM '
		consulta004=' and almacen.cor_tipo_zona_sec = tipo.cor_tipo_zona_sec;'
		
	consulta0= "SELECT z4.cor_zona_n4_id,z3.cor_zona_n3_id,z2.cor_zona_n2_id,z1.cor_zona_n1_id, " 
	consulta1=" almacen, cor_crm_zona_niv4 z4, cor_crm_zona_niv3 z3, cor_crm_zona_niv2 z2, cor_crm_zona_niv1 z1, "
	consulta2=""+alm+""" and almacen.cor_zona_n4_sec=z4.cor_zona_n4_sec
				   and almacen.cor_zona_n3_sec=z3.cor_zona_n3_sec and almacen.cor_zona_n2_sec=z2.cor_zona_n2_sec
				   and almacen.cor_zona_n1_sec=z1.cor_zona_n1_sec"""
	consulta0+=consultal4+""+consulta05+""+consulta1+""+consulta04+""+consulta15+""+consulta2+""+consulta004
	campos = ["z4","z3","z2","z1","tipo","sec"]
	parametros = []
	return consultas(TK, consulta0, campos, parametros,"consulta")
	
@route('/t_cor_inv_almacen_zona_0002_consulta_ubicacion_detalle')
def t_cor_inv_almacen_zona_0002_consulta_ubicacion_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	tipo = recibido["tipo"]
	alm = recibido["almacen"]
	if (tipo=='almacen'):
		consulta05='cor_inv_almacen_zona'
		consulta15='almacen.cor_almacen_sec='
		consulta04='cor_crm_tipo_zona tipo WHERE almacen.cor_almacen_zona_sec = %s and '
		consultal4='tipo.cor_tipo_zona_id, tipo.cor_tipo_zona_sec, almacen.cor_almacen_zona_sec, almacen.cor_almacen_zona_desc FROM '
		consulta004=' and almacen.cor_tipo_zona_sec = tipo.cor_tipo_zona_sec;'
	else:
		consulta05='cor_crm_responsable_zona'
		consulta15='almacen.cor_responsable_sec='
		consulta04='cor_crm_tipo_zona tipo WHERE almacen.cor_responsable_zona_sec = %s and '
		consultal4='tipo.cor_tipo_zona_id, tipo.cor_tipo_zona_sec, almacen.cor_responsable_zona_sec, almacen.cor_responsable_zona_desc FROM '
		consulta004=' and almacen.cor_tipo_zona_sec = tipo.cor_tipo_zona_sec;'
		
	consulta0= "SELECT z4.cor_zona_n4_id,z3.cor_zona_n3_id,z2.cor_zona_n2_id,z1.cor_zona_n1_id,z4.cor_zona_n4_sec,z3.cor_zona_n3_sec,z2.cor_zona_n2_sec,z1.cor_zona_n1_sec, " 
	consulta1=" almacen, cor_crm_zona_niv4 z4, cor_crm_zona_niv3 z3, cor_crm_zona_niv2 z2, cor_crm_zona_niv1 z1, "
	consulta2=""+alm+""" and almacen.cor_zona_n4_sec=z4.cor_zona_n4_sec
				   and almacen.cor_zona_n3_sec=z3.cor_zona_n3_sec and almacen.cor_zona_n2_sec=z2.cor_zona_n2_sec
				   and almacen.cor_zona_n1_sec=z1.cor_zona_n1_sec"""
	consulta0+=consultal4+""+consulta05+""+consulta1+""+consulta04+""+consulta15+""+consulta2+""+consulta004
	campos = ["z4","z3","z2","z1","z4s","z3s","z2s","z1s","tipo","tipos","sec","desc"]
	parametros = [recibido["sec"]]
	return consultas(TK, consulta0, campos, parametros,"consulta")
	
@route('/t_cor_inv_almacen_zona_0003_actualiza_ubicacion_detalle')
def t_cor_inv_almacen_zona_0003_actualiza_ubicacion_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	tipo = recibido["tipo"]
	consulta0 = ""
	if (tipo=='almacen'):
		consulta0 = """ UPDATE cor_inv_almacen_zona 
						SET cor_zona_n4_sec = %s, cor_zona_n3_sec = %s, cor_zona_n2_sec = %s, cor_zona_n1_sec = %s, cor_almacen_zona_desc = %s, cor_tipo_zona_sec = %s
						WHERE cor_almacen_zona_sec = %s"""
	else:
		consulta0 = """ UPDATE cor_crm_responsable_zona
						SET cor_zona_n4_sec = %s, cor_zona_n3_sec = %s, cor_zona_n2_sec = %s, cor_zona_n1_sec = %s, cor_responsable_zona_desc = %s, cor_tipo_zona_sec = %s
						WHERE cor_responsable_zona_sec = %s"""
		
	campos = []
	parametros = [recibido["z4"],recibido["z3"],recibido["z2"],recibido["z1"],recibido["desc"],recibido["tipo1"],recibido["almacen"]]
	return consultas(TK, consulta0, campos, parametros,"modifica")
			
#############################################################################################################################
####                                           TABLA cor_crm_tipo_responsable                                            ####
#############################################################################################################################		
@route('/t_cor_crm_tipo_responsable_0001_consulta_tipo_responsable')	
def ConsultaTipoResp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tiporesponsable_sec, cor_tiporesponsable_id FROM cor_crm_tipo_responsable WHERE cor_tiporesponsable_activo = 'S' ORDER BY cor_tiporesponsable_id; "
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_tipo_responsable_0002_consulta_tipo_responsable')	
def t_cor_crm_tipo_responsable_0002_consulta_tipo_responsable():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tiporesponsable_sec, cor_tiporesponsable_id FROM cor_crm_tipo_responsable ORDER BY cor_tiporesponsable_id; "
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_crm_tipo_responsable_0003_consulta_tipo_responsable_detalle')	
def t_cor_crm_tipo_responsable_0003_consulta_tipo_responsable_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tiporesponsable_id, cor_tiporesponsable_desc, cor_tiporesponsable_activo FROM cor_crm_tipo_responsable WHERE cor_tiporesponsable_sec = %s; "
	campos = ["id","desc","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_crm_tipo_responsable_0004_actualiza_tipo_responsable')	
def t_cor_crm_tipo_responsable_0004_actualiza_tipo_responsable():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_crm_tipo_responsable
				SET cor_tiporesponsable_fec_mod = %s, cor_tiporesponsable_ip_mod = %s, cor_tiporesponsable_login_mod = %s,
				cor_tiporesponsable_id = %s, cor_tiporesponsable_desc = %s, cor_tiporesponsable_activo = %s WHERE cor_tiporesponsable_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")

@route('/t_cor_crm_tipo_responsable_0005_inserta_tipo_responsable')
def t_cor_crm_tipo_responsable_0005_inserta_tipo_responsable():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_tipo_responsable (
            cor_tiporesponsable_sec, cor_tiporesponsable_id, cor_tiporesponsable_desc, cor_tiporesponsable_fec_mod, cor_tiporesponsable_ip_mod, cor_tiporesponsable_login_mod, cor_tiporesponsable_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_crm_tipo_responsable_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
#############################################################################################################################
####                                       TABLA cor_crm_responsable_mediocom                                            ####
#############################################################################################################################		
@route('/t_cor_crm_responsable_mediocom_0001_consulta_mediocom_responsable')
def ConsultaMedioCom():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """SELECT resp.cor_resp_mediocom_dato, me.cor_mediocom_id 
				   FROM cor_crm_responsable_mediocom resp, cor_crm_mediocom me
				   WHERE resp.cor_responsable_sec = %s and resp.cor_mediocom_sec=me.cor_mediocomsec ORDER BY me.cor_mediocom_id;"""
	campos = ["dato","id"]
	parametros = [recibido["resp"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
#New 19/09/2012 Alvaro	
@route('/t_cor_crm_responsable_mediocom_0001_insertar_mediocom_responsable')
def AgregarMC():
		
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_responsable_mediocom(
            cor_responsable_sec, cor_mediocom_sec, cor_resp_mediocom_sec, 
            cor_resp_mediocom_dato, cor_resp_mediocom_activo, cor_resp_mediocom_fec_mod, 
           cor_resp_mediocom_ip_mod, cor_resp_mediocom_login_mod)
    VALUES (%s, %s, %s, %s, 'S', %s, %s, %s);"""
	campos = []
	tiempo = time.localtime()
	ano = str(tiempo[0])
	mes = str(tiempo[1])
	dia = str(tiempo[2])
	hora = str(tiempo[3])
	minutos = str(tiempo[4])
	segundos = str(tiempo[5])
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("select nextval('cor_crm_responsable_mediocom_sec')")
	losDatos = miCursor.fetchall()
	parametros = [recibido["resp"],recibido["tipo"],losDatos[0][0],recibido["dato"],ano+"-"+mes+"-"+dia+" "+hora+":"+minutos+":"+segundos+"-4:30",ip_mod,recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")	
	
@route('/t_cor_crm_responsable_mediocom_0002_inserta_mediocom_responsable')
def AgregarMC(resp,dato,tipo,user):
	recibido = dict(request.GET)
	TK = recibido["TK"]
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_crm_responsable_mediocom_sec');")
	losDatos = miCursor.fetchall()
			
	query="""INSERT INTO cor_crm_responsable_mediocom(
            cor_responsable_sec, cor_mediocom_sec, cor_resp_mediocom_sec, 
            cor_resp_mediocom_dato, cor_resp_mediocom_activo, cor_resp_mediocom_fec_mod, 
            cor_resp_mediocom_ip_mod, cor_resp_mediocom_login_mod)
    VALUES (%s, %s, %s, %s,'S', %s, %s, %s);"""
	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	parametros = [recibido["resp"],recibido["tipo"],losDatos[0][0],recibido["dato"],fec_mod,ip_mod,recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
#############################################################################################################################
####                                                 TABLA cor_crm_tipo_zona                                             ####
#############################################################################################################################
@route('/t_cor_crm_tipo_zona_0001_consuta_tipo_zona')
def ZonaTipo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tipo_zona_sec,cor_tipo_zona_id FROM cor_crm_tipo_zona WHERE cor_tipo_zona_activo='S' ORDER BY cor_tipo_zona_id;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

#############################################################################################################################
####                                               TABLA t_cor_crm_zona_niv1                                             ####
#############################################################################################################################
@route('/t_cor_crm_zona_niv1_0001_consulta_zona_n1')
def NewZonaAlmacenz1():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_zona_n1_sec,cor_zona_n1_id FROM cor_crm_zona_niv1 WHERE cor_zona_n1_activo='S' ORDER BY cor_zona_n1_id;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
#############################################################################################################################
####                                               TABLA t_cor_crm_zona_niv2                                             ####
#############################################################################################################################
@route('/t_cor_crm_zona_niv2_0001_consulta_zona_n2')
def NewZonaAlmacenz2():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_zona_n2_sec,cor_zona_n2_id FROM cor_crm_zona_niv2 WHERE cor_zona_n2_activo='S' and cor_zona_n1_sec= %s ORDER BY cor_zona_n2_id;"
	campos = ["sec","id"]
	parametros = [recibido["pais"]]
	return consultas(TK, query, campos, parametros,"consulta")	

#############################################################################################################################
####                                               TABLA t_cor_crm_zona_niv3                                             ####
#############################################################################################################################
@route('/t_cor_crm_zona_niv2_0001_consulta_zona_n3')
def NewZonaAlmacenz3():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_zona_n3_sec,cor_zona_n3_id FROM cor_crm_zona_niv3 WHERE cor_zona_n3_activo = 'S' and cor_zona_n1_sec = %s and cor_zona_n2_sec = %s ORDER BY cor_zona_n3_id ;"
	campos = ["sec","id"]
	parametros = [recibido["pais"],recibido["est"]]
	return consultas(TK, query, campos, parametros,"consulta")		

#############################################################################################################################
####                                               TABLA t_cor_crm_zona_niv4                                             ####
#############################################################################################################################
@route('/t_cor_crm_zona_niv2_0001_consulta_zona_n4')
def NewZonaAlmacenz4():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_zona_n4_sec,cor_zona_n4_id FROM cor_crm_zona_niv4 WHERE cor_zona_n4_activo='S' and cor_zona_n1_sec=%s and cor_zona_n2_sec=%s and cor_zona_n3_sec=%s ORDER BY cor_zona_n4_id;"
	campos = ["sec","id"]
	parametros = [recibido["pais"],recibido["est"],recibido["sector"]]
	return consultas(TK, query, campos, parametros,"consulta")


@route('/AgregarZona')	
def AgregarZona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
		
	if(recibido["AorR"]=="Almacen"):
		conexion = psycopg2.connect(confBaseDatos)
		miCursor = conexion.cursor()
		miCursor.execute("SELECT nextval('cor_inv_almacen_zona_sec');")
		losDatos = miCursor.fetchall()
		
		query=""" INSERT INTO cor_inv_almacen_zona(
            cor_almacen_sec, cor_zona_n4_sec, cor_zona_n3_sec, cor_zona_n2_sec, 
            cor_zona_n1_sec, cor_almacen_zona_sec, cor_almacen_zona_desc, 
            cor_tipo_zona_sec, cor_almacen_zona_activo, cor_almacen_zona_fec_mod, 
            cor_almacen_zona_ip_md, cor_almacen_zona_login_mod)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
		
		campos = []
		parametros = [recibido["alm"],recibido["zona"],recibido["sector"],recibido["est"],recibido["pais"],losDatos[0][0],recibido["desc"],recibido["tipo"],"S",fech_mod,ip_mod,recibido["user"]]
		return consultas(TK, query, campos, parametros,"inserta")
	else:
		conexion = psycopg2.connect(confBaseDatos)
		miCursor = conexion.cursor()
		miCursor.execute("SELECT nextval('cor_crm_responsable_zona_sec');")
		losDatos = miCursor.fetchall()
		
		
		query=""" INSERT INTO cor_crm_responsable_zona(
            cor_responsable_sec, cor_zona_n4_sec, cor_zona_n3_sec, cor_zona_n2_sec, 
            cor_zona_n1_sec, cor_responsable_zona_sec, cor_responsable_zona_desc, 
            cor_tipo_zona_sec, cor_responsable_zona_activo, cor_responsable_zona_fec_mod, 
            cor_responsable_zona_ip_mod, cor_responsable_zona_login_mod)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	
		campos = []
		parametros = [recibido["alm"],recibido["zona"],recibido["sector"],recibido["est"],recibido["pais"],losDatos[0][0],recibido["desc"],recibido["tipo"],"S",fech_mod,ip_mod,recibido["user"]]
		return consultas(TK, query, campos, parametros,"inserta")
	
#############################################################################################################################
####                                                TABLA cor_inv_composicion                                            ####
#############################################################################################################################
@route('/t_cor_inv_composicion_0001_consulta_composicion_activo')
def t_cor_inv_composicion_0001_consulta_composicion_activo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_composicion_sec, cor_composicion_id FROM cor_inv_composicion WHERE cor_composicion_activo='S' ORDER BY cor_composicion_id;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/ t_cor_inv_composicion_0001_consulta_composicion_disp')
def t_cor_inv_composicion_0001_consulta_composicion_disp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT COUNT(*), cor_matcat_sec FROM cor_inv_articulo WHERE cor_matcat_sec = %s and cor_articulo_estatus != 139 GROUP BY cor_matcat_sec ORDER BY cor_composicion_id;"
	campos = ["disp","sec"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")	
	
#New 24/09/2012 Alvaro	
def verificarcomponentes(TK,art,matcat):

	query="SELECT cor_articulo_componente_sec FROM cor_inv_articulo_componente WHERE cor_articulo_compuesto_sec=%s;"
	campos = ["sec"]
	parametros = [art]
	resultado=eval(consultas(TK, query, campos, parametros,"consulta"))
	secs = '('
	count=0
	i=0
	while i < len(resultado):
		
		if (secs == '(') :
			secs += str(resultado[i]["sec"])
		else:
			secs += ', ' + str(resultado[i]["sec"])
		i+=1
	secs += ')'
	
	query="SELECT cor_articulo_sec,cor_articulo_compuesto FROM cor_inv_articulo WHERE cor_articulo_sec IN "+secs+" and cor_matcat_sec!=%s;"
	campos = ["sec","compuesto"]
	parametros = [matcat]
	resultado1 = eval(consultas(TK, query, campos, parametros,"consulta"))

	for result in resultado1:
		if(result["compuesto"]=="S"):
			count+=verificarcomponentes(TK,result["sec"],matcat)
	
	query="SELECT COUNT(*) FROM cor_inv_articulo WHERE cor_articulo_sec IN "+secs+" and cor_matcat_sec=%s;"
	campos = ["count"]
	parametros = [matcat]
	count+=int(eval(consultas(TK, query, campos, parametros,"consulta"))[0]["count"])
	return count
	
#New 24/09/2012 Alvaro
@route('/t_cor_inv_composicion_0002_consulta_composicion')
def ConsultaAlmacenComp():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	
	myReturnData = []
	dis=[]
	data=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	query="SELECT mat.cor_matcat_sec,mat.cor_matcat_id ,comp.cor_composicion_cantidad FROM cor_inv_composicion_detalle comp, cor_inv_matcat mat WHERE comp.cor_composicion_sec =%s and mat.cor_matcat_sec=comp.cor_matcat_sec;"
	campos=["sec","id","cant"]
	parametros=[recibido["comp"]]
	resultado=eval(consultas(TK, query, campos, parametros,"consulta"))
	
	for row in resultado:
		query="SELECT cor_articulo_sec,cor_articulo_compuesto,cor_matcat_sec FROM cor_inv_articulo WHERE cor_articulo_estatus=138 and cor_matcat_sec=%s"
		campos=["art_sec","compuesto","matcat_sec"]
		if (recibido["alm"]!=""):
			query+=" and cor_almacen_sec =%s ;"
			parametros=[row["sec"],recibido["alm"]]
		else:
			query+=";"
			parametros=[row["sec"]]
		resultado1=eval(consultas(TK, query, campos, parametros,"consulta"))
		count=len(resultado1)
		
		for i in resultado1:
			if (i["compuesto"]=="S"):
				count-=verificarcomponentes(TK,i["art_sec"],i["matcat_sec"])
		
		dis.append( operator.div(count,row["cant"]) )
		data.append([row["id"],row["cant"],count])
		
	minimo=min(dis)
	myReturnData.append({"dis":minimo})
	for row in data:
		res=row[2]-row[1]*minimo	
		myReturnData.append({"matcat":row[0],"res":res})
		
	return json.dumps(myReturnData)		

#############################################################################################################################
####                                               TABLA cor_crm_empinst_tipo                                            ####
#############################################################################################################################
@route('/t_cor_crm_empinst_tipo_0001_consulta_tipo_empisnt')
def t_cor_crm_empinst_tipo_0001_consulta_tipo_empisnt():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_empinst_tiposec, cor_empinst_tipo_id FROM cor_crm_empinst_tipo ORDER BY cor_empinst_tipo_id ;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_tipo_0002_consulta_tipo_empisnt_activo')
def t_cor_crm_empinst_tipo_0002_consulta_tipo_empisnt_activo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_empinst_tiposec, cor_empinst_tipo_id FROM cor_crm_empinst_tipo WHERE cor_empinst_tipo_activo = 'S' ORDER BY cor_empinst_tipo_id ;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_tipo_0003_consulta_tipo_empinst_detalle')	
def t_cor_crm_empinst_tipo_0003_consulta_tipo_empinst_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_empinst_tipo_id, cor_empinst_tipo_desc, cor_empinst_tipo_activo FROM cor_crm_empinst_tipo WHERE cor_empinst_tiposec = %s; "
	campos = ["id","desc","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_tipo_0004_actualiza_tipo_empisnt')	
def t_cor_crm_empinst_tipo_0004_actualiza_tipo_empisnt():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_crm_empinst_tipo
				SET cor_empinst_tipo_fec_mod = %s, cor_empinst_tipo_ip_mod = %s, cor_empinst_tipo_login_mod = %s,
				cor_empinst_tipo_id = %s, cor_empinst_tipo_desc = %s, cor_empinst_tipo_activo = %s WHERE cor_empinst_tiposec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_crm_empinst_tipo_0005_inserta_tipo_empisnt')
def t_cor_crm_empinst_tipo_0005_inserta_tipo_empisnt():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_empinst_tipo (
            cor_empinst_tiposec, cor_empinst_tipo_id, cor_empinst_tipo_desc, cor_empinst_tipo_fec_mod, cor_empinst_tipo_ip_mod, cor_empinst_tipo_login_mod, cor_empinst_tipo_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_crm_empinst_tipo_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
#############################################################################################################################
####                                                  TABLA cor_crm_empinst                                              ####
#############################################################################################################################
@route('/t_cor_crm_empinst_0001_consulta_beneficiario_activo')
def t_cor_crm_empinst_0001_consulta_beneficiario_activo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_empinstsec, cor_empinst_id FROM cor_crm_empinst WHERE cor_empinst_es_beneficiario='S' AND cor_empinst_activo='S' ORDER BY cor_empinst_id;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_0002_consulta_empinst')
def t_cor_crm_empinst_0002_consulta_empinst():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT a.cor_empinstsec, a.cor_empinst_id, a.cor_empinst_nombre, a.cor_empinst_es_cliente, a.cor_empinst_es_proveedor, a.cor_empinst_es_beneficiario, b.cor_empinst_tipo_id FROM cor_crm_empinst a, cor_crm_empinst_tipo b WHERE a.cor_empinst_tiposec = b.cor_empinst_tiposec ORDER BY cor_empinst_id;"
	campos = ["sec","id","nombre","cliente","prov","benef","tipo"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_0003_consulta_empinst_detalle')	
def t_cor_crm_empinst_0003_consulta_empinst_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "select a.cor_empinst_id, a.cor_empinst_nombre, a.cor_empinst_es_cliente, a.cor_empinst_es_proveedor, a.cor_empinst_es_beneficiario, a.cor_empinst_tiposec, a.cor_empinst_rif, a.cor_empinst_activo FROM cor_crm_empinst a WHERE a.cor_empinstsec = %s;"
	campos = ["id","nombre","cliente","prov","benef","tipo","rif","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_empinst_0004_actualiza_empisnt')	
def t_cor_crm_empinst_0004_actualiza_empisnt():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_crm_empinst
				SET cor_empinst_fec_mod = %s, cor_empinst_ip_mod = %s, cor_empinst_login_mod = %s,
				cor_empinst_id = %s, cor_empinst_nombre = %s, cor_empinst_activo = %s, cor_empinst_es_cliente = %s, cor_empinst_es_proveedor = %s, cor_empinst_es_beneficiario = %s,
				cor_empinst_tiposec = %s, cor_empinst_rif = %s WHERE cor_empinstsec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["nombre"],recibido["activo"],recibido["cliente"],recibido["prov"],recibido["benef"],recibido["tipo"],recibido["rif"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_crm_empinst_0005_inserta_empisnt')
def t_cor_crm_empinst_0005_inserta_empisnt():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_empinst (
            cor_empinstsec, cor_empinst_id, cor_empinst_nombre, cor_empinst_es_cliente, cor_empinst_es_proveedor, cor_empinst_tiposec, cor_empinst_activo, cor_empinst_fec_mod, cor_empinst_ip_mod, cor_empinst_login_mod, cor_empinst_rif, cor_empinst_es_beneficiario)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_crm_empinst_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],recibido["cliente"],recibido["prov"],recibido["tipo"],recibido["activo"],fech_mod,ip_mod,recibido["user"],recibido["rif"],recibido["benef"]]
	return consultas(TK, query, campos, parametros,"inserta")	
#############################################################################################################################
####                                                TABLA cor_inv_movimiento                                             ####
#############################################################################################################################
@route('/t_cor_inv_movimiento_0001_inserta_despacho')
def t_cor_inv_movimiento_0001_inserta_despacho():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_movimiento(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, cor_responsable_sec, cor_empinstsec,
			cor_movimiento_fec_mod, cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_tipo, cor_movimiento_compsicion, 
			cor_movimiento_cantidad, cor_movimiento_estatus, cor_almacen_sec)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'A', %s);"""
	campos = []
	fechah=recibido["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fechah,recibido["resp"],recibido["emp"],fech_mod,ip_mod,recibido["user"],"D",recibido["comp"],recibido["cant"],recibido["alm"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
@route('/t_cor_inv_movimiento_0002_consulta_despacho')
def t_cor_inv_movimiento_0002_consulta_despacho():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT lote.cor_movimiento_id, lote.cor_movimiento_desc, lote.cor_movimiento_fecha, lote.cor_responsable_sec, lote.cor_almacen_sec, lote.cor_movimiento_compsicion, lote.cor_empinstsec, lote.cor_movimiento_cantidad, resp.cor_responsable_nombre, alm.cor_almacen_id, comp.cor_composicion_id, empinst.cor_empinst_id, lote.cor_movimiento_estatus
				FROM cor_inv_movimiento as lote JOIN cor_crm_responsable resp ON lote.cor_responsable_sec = resp.cor_responsable_sec JOIN cor_inv_almacen alm ON lote.cor_almacen_sec = alm.cor_almacen_sec JOIN cor_inv_composicion comp ON lote.cor_movimiento_compsicion = comp.cor_composicion_sec JOIN cor_crm_empinst empinst ON lote.cor_empinstsec = empinst.cor_empinstsec
				WHERE lote.cor_movimiento_sec = %s;"""
	campos = ["id","desc","fecha","resp","alm","comp","emp","cant","resp_n","alm_n","comp_n","emp_n","estatus"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_movimiento_0003_actualiza_estatus_despacho')	
def t_cor_inv_movimiento_0003_actualiza_estatus_despacho():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_movimiento
				SET cor_movimiento_fec_mod = %s, cor_movimiento_ip_mod = %s, cor_movimiento_login_mod = %s,
				cor_movimiento_estatus='C' WHERE cor_movimiento_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
#############################################################################################################################
####                                       TABLA cor_inv_composicion_detalle                                             ####
#############################################################################################################################
@route('/t_cor_inv_composicion_detalle_0001_consulta_composicion')
def t_cor_inv_composicion_detalle_0001_consulta_composicion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT matcat.cor_matcat_sec,matcat.cor_matcat_id, comp.cor_composicion_cantidad 
				   FROM cor_inv_matcat matcat,cor_inv_composicion_detalle comp
				   WHERE matcat.cor_matcat_sec=comp.cor_matcat_sec and comp.cor_composicion_sec=%s ORDER BY matcat.cor_matcat_id;"""
	campos = ["sec","id","cant"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

#############################################################################################################################
####                                                   TABLA cor_inv_color                                               ####
#############################################################################################################################
@route('/t_cor_inv_color_0001_consulta_colores_activos')
def t_cor_inv_color_0001_consulta_colores_activos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_color_sec, cor_color_id FROM cor_inv_color WHERE cor_color_activo = 'S' ORDER BY cor_color_id"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_color_0002_consulta_colores')
def t_cor_inv_color_0002_consulta_colores():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_color_sec, cor_color_id FROM cor_inv_color ORDER BY cor_color_id"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_color_0003_consulta_color_detalle')
def t_cor_inv_color_0003_consulta_color_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_color_id, cor_color_desc, cor_color_activo FROM cor_inv_color WHERE cor_color_sec = %s"
	campos = ["id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_color_0004_actualiza_color')	
def t_cor_inv_color_0004_actualiza_color():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_color
				SET cor_color_fec_mod = %s, cor_color_ip_mod = %s, cor_color_login_mod = %s,
				cor_color_id = %s, cor_color_desc = %s, cor_color_activo = %s WHERE cor_color_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_color_0005_inserta_color')
def t_cor_inv_color_0005_inserta_color():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_color(
            cor_color_sec, cor_color_id, cor_color_desc, cor_color_fec_mod, cor_color_ip_mod, cor_color_login_mod, cor_color_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_color_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
##**
#############################################################################################################################
####                                             TABLA cor_inv_matcat_niv1                                               ####
#############################################################################################################################
@route('/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos')
def t_cor_inv_matcat_niv1_0001_consulta_grupo_activos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_activo = 'S'"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_matcat_niv1_0002_consulta_grupo')
def t_cor_inv_matcat_niv1_0002_consulta_grupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n1_id FROM cor_inv_matcat_niv1"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv1_0003_consulta_grupo_detalle')
def t_cor_inv_matcat_niv1_0003_consulta_grupo_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_id, cor_matcat_n1_desc, cor_matcat_n1_activo FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec = %s"
	campos = ["id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv1_0004_actualiza_grupo')	
def t_cor_inv_matcat_niv1_0004_actualiza_grupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_matcat_niv1
				SET cor_matcat_n1_fec_mod = %s, cor_matcat_n1_ip_mod = %s, cor_matcat_n1_login_mod = %s,
				cor_matcat_n1_id = %s, cor_matcat_n1_desc = %s, cor_matcat_n1_activo = %s WHERE cor_matcat_n1_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_matcat_niv1_0005_inserta_grupo')
def t_cor_inv_matcat_niv1_0005_inserta_grupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_matcat_niv1 (
            cor_matcat_n1_sec, cor_matcat_n1_id, cor_matcat_n1_desc, cor_matcat_n1_fec_mod, cor_matcat_n1_ip_mod, cor_matcat_n1_login_mod, cor_matcat_n1_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_matcat_niv1_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")

#############################################################################################################################
####                                             TABLA cor_inv_matcat_niv2                                               ####
#############################################################################################################################
@route('/t_cor_inv_matcat_niv2_0001_consulta_subgrupo_activos')
def t_cor_inv_matcat_niv2_0001_consulta_subgrupo_activos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n2_activo = 'S'"
	campos = ["n1","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_matcat_niv2_0002_consulta_subgrupo')
def t_cor_inv_matcat_niv2_0002_consulta_subgrupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT b.cor_matcat_n1_id, a.cor_matcat_n2_sec, a.cor_matcat_n2_id FROM cor_inv_matcat_niv2 a JOIN cor_inv_matcat_niv1 b ON a.cor_matcat_n1_sec = b.cor_matcat_n1_sec"
	campos = ["n1","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv2_0003_consulta_subgrupo_detalle')
def t_cor_inv_matcat_niv2_0003_consulta_subgrupo_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_id, cor_matcat_n2_desc, cor_matcat_n2_activo FROM cor_inv_matcat_niv2 WHERE cor_matcat_n2_sec = %s"
	campos = ["n1","id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv2_0004_actualiza_subgrupo')	
def t_cor_inv_matcat_niv2_0004_actualiza_subgrupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_matcat_niv2
				SET cor_matcat_n2_fec_mod = %s, cor_matcat_n2_ip_mod = %s, cor_matcat_n2_login_mod = %s,
				cor_matcat_n2_id = %s, cor_matcat_n2_desc = %s, cor_matcat_n2_activo = %s, cor_matcat_n1_sec = %s WHERE cor_matcat_n2_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["n1"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_matcat_niv2_0005_inserta_subgrupo')
def t_cor_inv_matcat_niv2_0005_inserta_subgrupo():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_matcat_niv2 (
            cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n2_id, cor_matcat_n2_desc, cor_matcat_n2_fec_mod, cor_matcat_n2_ip_mod, cor_matcat_n2_login_mod, cor_matcat_n2_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_matcat_niv2_sec')")
	losDatos = miCursor.fetchall()
	parametros = [recibido["n1"],losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
@route('/t_cor_inv_matcat_niv2_0006_consulta_subgrupo_activos')
def t_cor_inv_matcat_niv2_0006_consulta_subgrupo_activos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n2_activo = 'S' AND cor_matcat_n1_sec = %s"
	campos = ["n1","sec","id"]
	parametros = [recibido["n1"]]
	return consultas(TK, query, campos, parametros,"consulta")

#############################################################################################################################
####                                             TABLA cor_inv_matcat_niv3                                               ####
#############################################################################################################################
@route('/t_cor_inv_matcat_niv3_0001_consulta_clases_activas')
def t_cor_inv_matcat_niv3_0001_consulta_clases_activas():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n3_activo = 'S'"
	campos = ["n1","n2","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_matcat_niv3_0002_consulta_clase')
def t_cor_inv_matcat_niv3_0002_consulta_clase():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT b.cor_matcat_n1_id, c.cor_matcat_n2_id, a.cor_matcat_n3_sec, a.cor_matcat_n3_id FROM cor_inv_matcat_niv3 a JOIN cor_inv_matcat_niv1 b ON a.cor_matcat_n1_sec = b.cor_matcat_n1_sec JOIN cor_inv_matcat_niv2 c ON a.cor_matcat_n2_sec = c.cor_matcat_n2_sec"
	campos = ["n1","n2","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv3_0003_consulta_clase_detalle')
def t_cor_inv_matcat_niv3_0003_consulta_clase_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_id, cor_matcat_n3_desc, cor_matcat_n3_activo FROM cor_inv_matcat_niv3 WHERE cor_matcat_n3_sec = %s"
	campos = ["n1","n2","id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_niv3_0004_actualiza_clase')	
def t_cor_inv_matcat_niv3_0004_actualiza_clase():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_matcat_niv3
				SET cor_matcat_n3_fec_mod = %s, cor_matcat_n3_ip_mod = %s, cor_matcat_n3_login_mod = %s,
				cor_matcat_n3_id = %s, cor_matcat_n3_desc = %s, cor_matcat_n3_activo = %s, cor_matcat_n1_sec = %s, cor_matcat_n2_sec = %s WHERE cor_matcat_n3_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["n1"],recibido["n2"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_matcat_niv3_0005_inserta_clase')
def t_cor_inv_matcat_niv3_0005_inserta_clase():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_matcat_niv3 (
            cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_n3_id, cor_matcat_n3_desc, cor_matcat_n3_fec_mod, cor_matcat_n3_ip_mod, cor_matcat_n3_login_mod, cor_matcat_n3_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_matcat_niv3_sec')")
	losDatos = miCursor.fetchall()
	parametros = [recibido["n1"],recibido["n2"],losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")

@route('/t_cor_inv_matcat_niv3_0006_consulta_clases_activos')
def t_cor_inv_matcat_niv3_0006_consulta_clases_activos():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n3_activo = 'S' AND cor_matcat_n1_sec = %s and cor_matcat_n2_sec = %s"
	campos = ["n1","n2","sec","id"]
	parametros = [recibido["n1"],recibido["n2"]]
	return consultas(TK, query, campos, parametros,"consulta")

#############################################################################################################################
####                                                  TABLA cor_inv_matcat                                               ####
#############################################################################################################################
@route('/t_cor_inv_matcat_0001_consulta_material_activas')
def t_cor_inv_matcat_0001_consulta_material_activas():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_sec, cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_activo = 'S' ORDER BY cor_matcat_id"
	campos = ["n1","n2","n3","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_inv_matcat_0002_consulta_material')
def t_cor_inv_matcat_0002_consulta_material():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT b.cor_matcat_n1_id, c.cor_matcat_n2_id, d.cor_matcat_n3_id, a.cor_matcat_sec, a.cor_matcat_id FROM cor_inv_matcat a JOIN cor_inv_matcat_niv1 b ON a.cor_matcat_n1_sec = b.cor_matcat_n1_sec JOIN cor_inv_matcat_niv2 c ON a.cor_matcat_n2_sec = c.cor_matcat_n2_sec JOIN cor_inv_matcat_niv3 d ON a.cor_matcat_n3_sec = d.cor_matcat_n3_sec"
	campos = ["n1","n2","n3","sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_0003_consulta_material_detalle')
def t_cor_inv_matcat_0003_consulta_material_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_id, cor_matcat_desc, cor_matcat_activo FROM cor_inv_matcat WHERE cor_matcat_sec = %s"
	campos = ["n1","n2","n3","id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_matcat_0004_actualiza_material')	
def t_cor_inv_matcat_0004_actualiza_material():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_matcat
				SET cor_matcat_fec_mod = %s, cor_matcat_ip_mod = %s, cor_matcat_login_mod = %s,
				cor_matcat_id = %s, cor_matcat_desc = %s, cor_matcat_activo = %s, cor_matcat_n1_sec = %s, cor_matcat_n2_sec = %s, cor_matcat_n3_sec = %s WHERE cor_matcat_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["n1"],recibido["n2"],recibido["n3"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_matcat_0005_inserta_material')
def t_cor_inv_matcat_0005_inserta_material():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_matcat (
            cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_matcat_sec, cor_matcat_id, cor_matcat_desc, cor_matcat_fec_mod, cor_matcat_ip_mod, cor_matcat_login_mod, cor_matcat_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_matcat_sec')")
	losDatos = miCursor.fetchall()
	parametros = [recibido["n1"],recibido["n2"],recibido["n3"],losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
#############################################################################################################################
####                                                TABLA cor_crm_mediocom                                               ####
#############################################################################################################################
@route('/t_cor_crm_mediocom_0001_consulta_medios_comunicacion_activos')
def CMC():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_mediocomsec,cor_mediocom_id FROM cor_crm_mediocom WHERE cor_mediocom_activo='S';"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")


#############################################################################################################################
####                                             TABLA cor_inv_materia_prima                                            ####
#############################################################################################################################
@route('/t_cor_inv_materia_prima_0001_consulta_materia_prima_activa')
def t_cor_inv_materia_prima_0001_consulta_materia_prima_activa():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matprima_sec, cor_matprima_id FROM cor_inv_materia_prima WHERE cor_matprima_activo = 'S'"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_materia_prima_0002_consulta_materia_prima')
def t_cor_inv_materia_prima_0002_consulta_materia_prima():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matprima_sec, cor_matprima_id FROM cor_inv_materia_prima"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_materia_prima_0003_consulta_materia_prima_detalle')
def t_cor_inv_materia_prima_0003_consulta_materia_prima_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_matprima_id, cor_matprima_desc, cor_matprima_activo FROM cor_inv_materia_prima WHERE cor_matprima_sec = %s"
	campos = ["id","desc","activo"]
	parametros = [recibido["mp"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_materia_prima_0004_actualiza_materia_prima')	
def t_cor_inv_materia_prima_0004_actualiza_materia_prima():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_inv_materia_prima
				SET cor_matprima_fec_mod = %s, cor_matprima_ip_mod = %s, cor_matprima_login_mod = %s,
				cor_matprima_id = %s, cor_matprima_desc = %s, cor_matprima_activo = %s WHERE cor_matprima_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
	
@route('/t_cor_inv_materia_prima_0005_inserta_materia_prima')
def t_cor_inv_materia_prima_0005_inserta_materia_prima():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_materia_prima(
            cor_matprima_sec, cor_matprima_id, cor_matprima_desc, cor_matprima_fec_mod, cor_matprima_ip_mod, cor_matprima_login_mod, cor_matprima_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_materia_prima_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")
#############################################################################################################################
####                                        TABLA cor_inv_movimiento_detalle                                             ####
#############################################################################################################################
@route('/t_cor_inv_movimiento_detalle_0003_consulta_despacho')
def t_cor_inv_movimiento_detalle_0003_consulta_despacho():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT COUNT(*) FROM cor_inv_movimiento_detalle WHERE cor_movimiento_sec=%s and cor_matcat_sec=%s;"
	campos = ["despachados"]
	parametros = [recibido["mov"],recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"consulta")
#############################################################################################################################
@route('/ConsultaDesp/<sec>')
def ConsultaDesp(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec,lote.cor_contactosec,lote.cor_movimiento_compsicion,lote.cor_empinstsec,lote.cor_movimiento_cantidad
				   FROM cor_inv_movimiento as lote
				   WHERE lote.cor_movimiento_sec =	"""+sec+";")
	result = cur.fetchone()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		cur.execute("SELECT resp.cor_responsable_nombre,cont.cor_contacto_nombre,comp.cor_composicion_id,emp.cor_empinst_id FROM cor_crm_responsable resp,cor_crm_contacto cont,cor_inv_composicion comp,cor_crm_empinst emp WHERE resp.cor_responsable_sec ="+ str(result[3])+" and cont.cor_contactosec="+ str(result[4])+" and comp.cor_composicion_sec="+ str(result[5])+" and emp.cor_empinstsec="+ str(result[6])+";")
		result1 = cur.fetchone()
		myReturnData.append({"id":result[0],"desc":result[1],"fecha":str(result[2]),"resp":result1[0],"contacto":result1[1],"compid":result1[2],"emp":result1[3],"comp":result[5],"cant":result[7]})

	return json.dumps(myReturnData)

#############################################################################################################################
####                                                        DESPACHOS                                                    ####
#############################################################################################################################	

#Modificado 19/09/2012 Alvaro
@route('/Despacho')
def Despacho():
	recibido=dict(request.GET)
	TK = recibido["TK"]
	CI = int(recibido["CI"])
	CF = recibido["CF"]
	xcod = eval(recibido["xcod"])
	
	if (CF == ""):
		CF = int(recibido["CI"])
	else:
		CF = int(recibido["CF"])
	codigos = '('
	i = CI;
	while i <= CF:
		c = str(i)
		if (codigos == '(') :
			codigos = codigos + c
		else:
			codigos = codigos + ', ' + str(i)
		i+=1
	codigos = codigos +')'
	query = """	SELECT cor_matcat_sec,COUNT(*)
				FROM cor_inv_articulo
				WHERE cor_articulo_sec IN """+codigos+"""
				GROUP BY cor_matcat_sec;"""
	campos = ["sec","cant"]
	parametros = []
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	
	query = """ SELECT cor_articulo_sec,cor_matcat_sec
				FROM cor_inv_articulo
				WHERE cor_articulo_sec IN """+codigos+"""
				ORDER BY cor_articulo_sec DESC;"""
	campos = ["sec","matcat"]
	parametros = []
	resultado1 = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	
	realcodigos = eval(codigos.replace("(","[").replace(")","]"))
	removed = []	
		
	for matcat in xcod:
		for matcat1 in resultado:
			if(matcat[0]==matcat1["sec"] and matcat[1]<matcat1["cant"]):
				cant=matcat1["cant"]-matcat[1]
				for i in resultado1:
					if(i["matcat"]==matcat[0]):
						removed.append({"sec":i["sec"]})
						realcodigos.remove(i["sec"])
						cant-=1
					if(cant==0):
						break
				break
	
	
	for ini in realcodigos:
		auxdespachar(recibido["mov"],ini,recibido["user"])

	return json.dumps(removed)


def auxdespachar(mov,row,user):
	#recibido = dict(request.GET)
	TK = tokenSistema
	query = "SELECT cor_matcat_sec,cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec,cor_articulo_compuesto FROM cor_inv_articulo WHERE cor_articulo_sec = %s;"
	campos = ["matcat","matcat_n1","matcat_n2","matcat_n3","compuesto"]
	parametros = [str(row)]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	if resultado != "":
		## ACTUALIZA EL ESTATUS DEL ARTICULO
		fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
		ip_mod=str(socket.gethostbyname(socket.gethostname()))
		query = """ UPDATE cor_inv_articulo
					SET cor_articulo_fec_mod = %s, cor_articulo_ip_mod = %s, cor_articulo_login_mod = %s, cor_articulo_estatus = 139
					WHERE cor_articulo_sec = %s;"""
		campos = []
		parametros = [fech_mod,ip_mod,user,str(row)]
		print consultas(TK, query, campos, parametros,"modifica")
		
		## INSERTO EL DETALLE PARA EL DESPACHO
		conexion = psycopg2.connect(confBaseDatos)
		miCursor = conexion.cursor()
		miCursor.execute("SELECT nextval('cor_inv_movimiento_detalle_sec');")
		losDatos = miCursor.fetchall()
		
		query = """INSERT INTO cor_inv_movimiento_detalle(
					cor_movimiento_sec, cor_articulo_sec, cor_movdetalle_fec_mod, cor_movdetalle_ip_mod, cor_movdetalle_login_mod, cor_matcat_sec, cor_movdetalle_sec)
				VALUES (%s, %s, %s, %s, %s, %s, %s);"""
		campos = []
		parametros = [mov,str(row),fech_mod,ip_mod,user,str(resultado[0]["matcat"]),losDatos[0][0]]
		print consultas(TK, query, campos, parametros,"inserta")
		
		if resultado[0]["compuesto"] == "S":
			## INSERTO SUS COMPONENTES
			despacharcompuesto(mov,row,user)
		return ""

def despacharcompuesto(mov,sec,user):
	TK = tokenSistema
	query = "SELECT cor_articulo_componente_sec FROM cor_inv_articulo_componente WHERE cor_articulo_compuesto_sec = "+str(sec)+";"
	campos = ["componente"]
	parametros = []
	resultado = eval(consultas(TK, query, campos, parametros,"consulta")+"")
	print query
	for row in resultado:
		auxdespachar(str(mov),row[0]["componente"],str(user))

#############################################################################################################################
# COMPOSICION 17/09/2012 12:08

@route('/t_cor_inv_composicion_0002_consulta_composiciones')
def ConsultaComposicion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_composicion_sec,cor_composicion_id FROM cor_inv_composicion;"
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_composicion_0003_inserta_composicion')
def t_cor_inv_composicion_0003_inserta_composicion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_composicion( 
				cor_composicion_sec, cor_composicion_id, cor_composicion_desc, cor_composicion_activo, cor_composicion_fec_mod, cor_composicion_ip_mod, cor_composicion_login_mod)
				VALUES (%s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_composicion_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],recibido["activo"],datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"),str(socket.gethostbyname(socket.gethostname())),recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")

@route('/t_cor_inv_composicion_0004_consulta_composicion') 
def t_cor_inv_composicion_0004_consulta_composicion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_composicion_sec, cor_composicion_id, cor_composicion_desc, cor_composicion_activo FROM cor_inv_composicion WHERE cor_composicion_sec = %s;"
	campos = ["sec","id","desc","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_composicion_0005_actualiza_composicion')
def t_cor_inv_composicion_0005_actualiza_composicion():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "UPDATE cor_inv_composicion SET cor_composicion_id = %s, cor_composicion_desc = %s, cor_composicion_activo = %s WHERE cor_composicion_sec = %s;"
	campos = []
	parametros = [recibido["id"],recibido["desc"],recibido["act"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")
  
@route('/t_cor_inv_composicion_detalle_0001_consulta_composicion_detalles')
def t_cor_inv_composicion_detalle_0001_consulta_composicion_detalles():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT comp.cor_matcat_sec, comp.cor_composicion_cantidad, matcat.cor_matcat_id FROM cor_inv_composicion_detalle comp JOIN cor_inv_matcat matcat ON comp.cor_matcat_sec = matcat.cor_matcat_sec WHERE cor_composicion_sec = %s;"
	campos = ["matcat","cant","id"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_composicion_detalle_0002_inserta_material')
def t_cor_inv_composicion_detalle_0002_inserta_material():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_composicion_detalle( 
				cor_composicion_sec, cor_matcat_sec, cor_composicion_cantidad, cor_compdetalle_fec_mod, cor_compdetalle_ip_mod, cor_compdetalle_login_mod)
				VALUES (%s, %s, %s, %s, %s, %s);"""
	campos = []
	parametros = [recibido["comp"],recibido["matcat"],recibido["cant"],datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"),str(socket.gethostbyname(socket.gethostname())),recibido["user"]]
	return consultas(TK, query, campos, parametros,"inserta")
	
@route('/t_cor_inv_composicion_detalle_0003_elimina_material')
def t_cor_inv_composicion_detalle_0003_elimina_material():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "DELETE FROM cor_inv_composicion_detalle WHERE cor_composicion_sec = %s and cor_matcat_sec = %s;"
	campos = []
	parametros = [recibido["comp"],recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"elimina")

@route('/t_cor_inv_composicion_detalle_0004_consulta_composicion_detalles')
def t_cor_inv_composicion_detalle_0004_consulta_composicion_detalles():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT comp.cor_composicion_cantidad, matcat.cor_matcat_id FROM cor_inv_composicion_detalle comp JOIN cor_inv_matcat matcat ON comp.cor_matcat_sec = matcat.cor_matcat_sec WHERE cor_composicion_sec = %s and comp.cor_matcat_sec = %s;"
	campos = ["cant","id"]
	parametros = [recibido["sec"],recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_inv_composicion_detalle_0005_actualiza_cantidad')
def t_cor_inv_composicion_detalle_0005_actualiza_cantidad():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "UPDATE cor_inv_composicion_detalle SET cor_composicion_cantidad = %s WHERE cor_composicion_sec = %s and cor_matcat_sec = %s;"
	campos = []
	parametros = [recibido["cant"],recibido["sec"],recibido["matcat"]]
	return consultas(TK, query, campos, parametros,"modifica")
#############################################################################################################################

#New 24/09/2012 Alvaro
@route("/t_cor_inv_movimiento_0001_inserta_inventario")
def t_cor_inv_movimiento_0001_inserta_inventario():
	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_inv_movimiento(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_responsable_sec,cor_almacen_sec, cor_movimiento_fec_mod, 
            cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_tipo,cor_movimiento_estatus)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'A');"""
	campos = []
	fechah=recibido["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fechah,recibido["resp"],recibido["almacen"],fech_mod,ip_mod,recibido["user"],"I"]
	return consultas(TK, query, campos, parametros,"inserta")

#New 24/09/2012 Alvaro
@route("/t_cor_inv_movimiento_0002_consulta_inventario")
def t_cor_inv_movimiento_0002_consulta_inventario():
	
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """	SELECT lote.cor_movimiento_id, lote.cor_movimiento_desc, lote.cor_movimiento_fecha, lote.cor_responsable_sec, lote.cor_almacen_sec, resp.cor_responsable_nombre, alm.cor_almacen_id, lote.cor_movimiento_estatus
				FROM cor_inv_movimiento as lote JOIN cor_crm_responsable resp ON lote.cor_responsable_sec = resp.cor_responsable_sec JOIN cor_inv_almacen alm ON lote.cor_almacen_sec = alm.cor_almacen_sec 
				WHERE lote.cor_movimiento_sec = %s;"""
	campos = ["id","desc","fecha","resp","alm","resp_n","alm_n","estatus"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")

#New 24/09/2012 Alvaro
@route('/ListaInventario')
def ListaInventario():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	
	query="""SELECT mat.cor_matcat_id,mov.cantidad 
				   FROM cor_inv_movimiento_detalle mov,cor_inv_matcat mat
				   WHERE mov.cor_movimiento_sec=%s and mat.cor_matcat_sec=mov.cor_matcat_sec;"""
	campos = ["id","cant"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")
		

#New 24/09/2012 Alvaro	
@route('/ValidarInventario')
def ValidarInventario():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	myReturnData = []
	
	query="""SELECT cor_matcat_sec
		   FROM cor_inv_movimiento_detalle 
		   WHERE cor_movimiento_sec=%s;"""

	campos = ["sec"]
	parametros = [recibido["mov"]]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta"))
	
	for row in resultado:
		if(int(recibido["matcat"])==row["sec"]):
			myReturnData.append({"error":'true'})
			return json.dumps(myReturnData)
	myReturnData.append({"error":'false'})
	return json.dumps(myReturnData)
	
#New 24/09/2012 Alvaro
@route('/RegInventario')
def RegInventario():

	recibido = dict(request.GET)
	TK = recibido["TK"]	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	query="SELECT cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec FROM cor_inv_matcat WHERE cor_matcat_sec=%s;"
	campos = ["sec_n1","sec_n2","sec_n3"]
	parametros = [recibido["matcat"]]
	resultado = eval(consultas(TK, query, campos, parametros,"consulta"))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_inv_movimiento_detalle_sec')")
	losDatos = miCursor.fetchall()
	
	query=""" INSERT INTO cor_inv_movimiento_detalle(
            cor_movimiento_sec, cor_movdetalle_fec_mod, 
            cor_movdetalle_ip_mod, cor_movdetalle_login_mod, cor_matcat_sec, 
            cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cantidad, 
            cor_movdetalle_sec)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
	
	campos = []
	parametros = [recibido["mov"],fech_mod,ip_mod,recibido["user"],recibido["matcat"],resultado[0]["sec_n1"],resultado[0]["sec_n2"],resultado[0]["sec_n3"],recibido["cant"],losDatos[0][0]]	
	
	return consultas(TK, query, campos, parametros,"inserta")

#############################################################################################################################
####                                                  TABLA cor_crm_tipo_zona                                            ####
#############################################################################################################################		
@route('/t_cor_crm_tipo_zona_0001_consulta_tipo_zona')	
def t_cor_crm_tipo_zona_0001_consulta_tipo_zona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tipo_zona_sec, cor_tipo_zona_id FROM cor_crm_tipo_zona WHERE cor_tipo_zona_activo = 'S'; "
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")

@route('/t_cor_crm_tipo_zona_0002_consulta_tipo_zona')	
def t_cor_crm_tipo_zona_0002_consulta_tipo_zona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tipo_zona_sec, cor_tipo_zona_id FROM cor_crm_tipo_zona; "
	campos = ["sec","id"]
	parametros = []
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_crm_tipo_zona_0003_consulta_tipo_zona_detalle')	
def t_cor_crm_tipo_zona_0003_consulta_tipo_zona_detalle():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = "SELECT cor_tipo_zona_id, cor_tipo_zona_desc, cor_tipo_zona_activo FROM cor_crm_tipo_zona WHERE cor_tipo_zona_sec = %s; "
	campos = ["id","desc","activo"]
	parametros = [recibido["sec"]]
	return consultas(TK, query, campos, parametros,"consulta")
	
@route('/t_cor_crm_tipo_zona_0004_actualiza_tipo_zona')	
def t_cor_crm_tipo_zona_0004_actualiza_tipo_zona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ UPDATE cor_crm_tipo_zona
				SET cor_tipo_zona_fec_mod = %s, cor_tipo_zona_ip_mod = %s, cor_tipo_zona_login_mod = %s,
				cor_tipo_zona_id = %s, cor_tipo_zona_desc = %s, cor_tipo_zona_activo = %s WHERE cor_tipo_zona_sec = %s;"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))	
	parametros = [fech_mod,ip_mod,recibido["user"],recibido["id"],recibido["desc"],recibido["activo"],recibido["sec"]]
	return consultas(TK, query, campos, parametros,"modifica")

@route('/t_cor_crm_tipo_zona_0005_inserta_tipo_zona')
def t_cor_crm_tipo_zona_0005_inserta_tipo_zona():
	recibido = dict(request.GET)
	TK = recibido["TK"]
	query = """ INSERT INTO cor_crm_tipo_zona (
            cor_tipo_zona_sec, cor_tipo_zona_id, cor_tipo_zona_desc, cor_tipo_zona_fec_mod, cor_tipo_zona_ip_mod, cor_tipo_zona_login_mod, cor_tipo_zona_activo)
    VALUES ( %s, %s, %s, %s, %s, %s, %s);"""
	campos = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	conexion = psycopg2.connect(confBaseDatos)
	miCursor = conexion.cursor()
	miCursor.execute("SELECT nextval('cor_crm_tipo_zona_sec')")
	losDatos = miCursor.fetchall()
	parametros = [losDatos[0][0],recibido["id"],recibido["desc"],fech_mod,ip_mod,recibido["user"],recibido["activo"]]
	return consultas(TK, query, campos, parametros,"inserta")


run(host=socket.gethostname(), port=8002)
