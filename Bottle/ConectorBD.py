from bottle import hook, response, route, run, static_file, request
import psycopg2, json, datetime, socket, operator
#from lib import *

@hook('after_request')
def enable_cors():
	response.headers['Access-Control-Allow-Origin'] = '*'

# ************************************************* #
# **         WEB SERVERS - Alvaro Parada         ** #
# ************************************************* #
	
@route('/PreConsultaArticulo')
def PreConsultaArticulo():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_articulo_sec,cor_articulo_id FROM cor_inv_articulo;")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})

	return json.dumps(myReturnData)
		
@route('/ConsultaMatcat/')
def ConsultaMatcat():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_matcat_sec,cor_matcat_id FROM cor_inv_matcat ;")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	
	return json.dumps(myReturnData)	
	
@route('/ConsultaTodos/<n>')
def ConsultaTodos(n):
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT cor_almacen_sec,COUNT(*) 
				  FROM cor_inv_articulo
				  WHERE cor_matcat_sec="""+n+""" and cor_articulo_estatus !=139
				  GROUP BY cor_almacen_sec;""")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_sec="+str(row[0])+";")
			result1=cur.fetchone()
			myReturnData.append({"id":result1[0],"num":row[1]})

	return json.dumps(myReturnData)		
	
@route('/ConsultaAlmacen')
def ConsultaAlmacen():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_almacen_sec,cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_activo='S';")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})

	return json.dumps(myReturnData)
	
@route('/CAlmacen')
def CAlmacen():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT alm.cor_almacen_sec,alm.cor_almacen_id,res.cor_responsable_nombre
				   FROM cor_inv_almacen alm,cor_crm_responsable res
				   WHERE alm.cor_responsable_sec=res.cor_responsable_sec;""")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1],"resp":row[2]})
	
	return json.dumps(myReturnData)	
	
@route('/CAlmacenaux/<sec>')
def CAlmacenaux(sec):
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT alm.cor_almacen_id,alm.cor_almacen_desc,alm.cor_almacen_tipo,alm.cor_responsable_sec,alm.cor_almacen_activo
				   FROM cor_inv_almacen alm
				   WHERE alm.cor_almacen_sec="""+sec+";")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"id":row[0],"desc":row[1],"tipo":row[2],"resp":row[3],"activo":row[4]})
	
	return json.dumps(myReturnData)	
	
	
@route('/ConsultaArtA/<n>/<almacen>')
def ConsultaArtA(n,almacen):
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT cor_almacen_sec,COUNT(*) 
				  FROM cor_inv_articulo
				  WHERE cor_matcat_sec="""+n+" and cor_articulo_estatus !=139 and cor_almacen_sec="+almacen+"""
				  GROUP BY cor_almacen_sec;""")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_sec="+str(row[0])+";")
			result1=cur.fetchone()
			myReturnData.append({"id":result1[0],"num":row[1]})

	return json.dumps(myReturnData)	
	
@route('/ConsultaComposicion')
def ConsultaComposicion():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_composicion_sec,cor_composicion_id FROM cor_inv_composicion;")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	
	return json.dumps(myReturnData)	
	
@route('/ConsultaTodosComp/<comp_sec>')
def ConsultaTodosComp(comp_sec):
	myReturnData=[]
	dis=[]
	data=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_matcat_sec,cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec,cor_composicion_cantidad FROM cor_inv_composicion_detalle WHERE cor_composicion_sec ="+comp_sec+";")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT COUNT(*) FROM cor_inv_articulo WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+" and cor_articulo_estatus != 139;")
			result1=cur.fetchone()
			dis.append( operator.div(result1[0],row[4]) )
			data.append([row[0],row[1],row[2],row[3],row[4],result1[0]])
		minimo=min(dis)
		myReturnData.append({"dis":minimo})
		for row in data:
			res=row[5]-row[4]*minimo
			cur.execute("SELECT cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result2=cur.fetchone()
			cur.execute("SELECT cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec="+str(row[1])+";")
			result3=cur.fetchone()
			cur.execute("SELECT cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+";")
			result4=cur.fetchone()
			cur.execute("SELECT cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result5=cur.fetchone()			
			myReturnData.append({"matcat":result2[0],"matcat_n1":result3[0],"matcat_n2":result4[0],"matcat_n3":result5[0],"res":res})
		
	return json.dumps(myReturnData)	
	
@route('/ConsultaAlmacenComp/<comp_sec>/<almacen>')
def ConsultaAlmacenComp(comp_sec,almacen):
	myReturnData=[]
	dis=[]
	data=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_matcat_sec,cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec,cor_composicion_cantidad FROM cor_inv_composicion_detalle WHERE cor_composicion_sec ="+comp_sec+";")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT COUNT(*) FROM cor_inv_articulo WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+" and cor_almacen_sec ="+almacen+" ;")
			result1=cur.fetchone()
			dis.append( operator.div(result1[0],row[4]) )
			data.append([row[0],row[1],row[2],row[3],row[4],result1[0]])
		minimo=min(dis)
		myReturnData.append({"dis":minimo})
		for row in data:
			res=row[5]-row[4]*minimo
			cur.execute("SELECT cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result2=cur.fetchone()
			cur.execute("SELECT cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec="+str(row[1])+";")
			result3=cur.fetchone()
			cur.execute("SELECT cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+";")
			result4=cur.fetchone()
			cur.execute("SELECT cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
			result5=cur.fetchone()			
			myReturnData.append({"matcat":result2[0],"matcat_n1":result3[0],"matcat_n2":result4[0],"matcat_n3":result5[0],"res":res})
			
	return json.dumps(myReturnData)	
	
@route('/CRecepcion/<FI>/<FF>/<sec>')
def CRecepcion(FI,FF,sec):	
	myReturnData=[]
	
	consulta="""SELECT lote.cor_movimiento_sec, lote.cor_movimiento_nro_recepcion, lote.cor_responsable_sec, lote.cor_movimiento_fecha, sum(lote_det.cor_movlote_cantidad), lote.cor_movimiento_estatus
				   FROM cor_inv_movimiento_lote as lote,cor_inv_movimiento_lote_detalle as lote_det 
				   WHERE lote.cor_movimiento_sec =lote_det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus != 'P' and lote.cor_movimiento_fecha between '"""+FI+" 00:00:00' and '"+FF+" 23:59:59'"
						
	consulta2=" GROUP BY lote.cor_movimiento_sec, lote.cor_movimiento_nro_recepcion, lote.cor_responsable_sec, lote.cor_movimiento_fecha, lote.cor_movimiento_estatus;"
	
		
	if( int(sec) > 0 ):
		consulta+="and lote.cor_factura_sec="+sec+""+consulta2
	else:
		consulta+=consulta2
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute(consulta)
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[2])+";")
			result1 = cur.fetchone()
			if(row[5]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"sec":row[0],"nro":row[1],"resp":result1[0],"fecha":str(row[3]),"cant":row[4],"estatus":est})
	
	return json.dumps(myReturnData)

@route('/ConsultaRecep/<sec>')
def ConsultaRecep(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_nro_recepcion, lote.cor_factura_sec, lote.cor_movimiento_fecha,lote.cor_movimiento_estatus, lote.cor_responsable_sec, sum(lote_det.cor_movlote_cantidad)
				   FROM cor_inv_movimiento_lote as lote,cor_inv_movimiento_lote_detalle as lote_det
				   WHERE lote.cor_movimiento_sec =lote_det.cor_movimiento_sec and lote.cor_movimiento_sec =	"""+sec+"""
				   GROUP BY lote.cor_movimiento_nro_recepcion, lote.cor_factura_sec, lote.cor_movimiento_fecha,lote.cor_movimiento_estatus, lote.cor_responsable_sec;""")
	result = cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("""SELECT fac.cor_factura_nro,cor_responsable_nombre,sum(det.cor_detfactura_cantidad_pend) 
						FROM cor_inv_factura fac, cor_inv_det_factura det, cor_crm_responsable
						WHERE fac.cor_factura_sec = """+str(row[1])+" and fac.cor_factura_sec = det.cor_factura_sec and cor_responsable_sec="+str(row[4])+""" 
						GROUP BY fac.cor_factura_nro,cor_responsable_nombre;""")
			result1= cur.fetchone()
			if(row[3]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"nro":row[0],"fac":result1[0],"fecha":str(row[2]),"estatus":est,"resp":result1[1],"recibir":result1[2],"recibidos":row[5],"facsec":row[1]})
		cur.execute("""SELECT det.cor_matcat_sec,det.cor_matcat_n1_sec,det.cor_matcat_n2_sec,det.cor_matcat_n3_sec, det.cor_movlote_cantidad,det.cor_almacen_sec,lote.cor_factura_sec
					FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
					WHERE lote.cor_movimiento_sec = det.cor_movimiento_sec and det.cor_movimiento_sec = """+sec+";")	
		result2 = cur.fetchall()
		if (result2==None):
			print(json.dumps(myReturnData))
			#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
		else:
			for row in result2:
				cur.execute("SELECT cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
				result6=cur.fetchone()
				cur.execute("SELECT cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec="+str(row[1])+";")
				result3=cur.fetchone()
				cur.execute("SELECT cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+";")
				result4=cur.fetchone()
				cur.execute("SELECT cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
				result5=cur.fetchone()
				cur.execute("SELECT cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_sec="+str(row[5])+";")
				result7=cur.fetchone()
				cur.execute("SELECT cor_detfactura_cantidad_pend FROM cor_inv_det_factura WHERE cor_factura_sec ="+ str(row[6])+" and cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+" ;")
				result8=cur.fetchone()
				myReturnData.append({"material":result6[0],"grupo":result3[0],"subgrupo":result4[0],"clase":result5[0],"unidades":row[4],"almacen":result7[0],"faltan":result8[0]})
	
	return json.dumps(myReturnData)
	
@route('/ConsultaInv/<sec>')
def ConsultaInv(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_nro_recepcion, lote.cor_factura_sec, lote.cor_movimiento_fecha,lote.cor_movimiento_estatus, lote.cor_responsable_sec, sum(lote_det.cor_movlote_cantidad)
				   FROM cor_inv_movimiento_lote as lote,cor_inv_movimiento_lote_detalle as lote_det
				   WHERE lote.cor_movimiento_sec =lote_det.cor_movimiento_sec and lote.cor_movimiento_sec =	"""+sec+"""
				   GROUP BY lote.cor_movimiento_nro_recepcion, lote.cor_factura_sec, lote.cor_movimiento_fecha,lote.cor_movimiento_estatus, lote.cor_responsable_sec;""")
	result = cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			if(row[3]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"nro":row[0],"fecha":str(row[2]),"estatus":est,"recibidos":row[5],"facsec":row[1]})
		cur.execute("""SELECT det.cor_matcat_sec,det.cor_matcat_n1_sec,det.cor_matcat_n2_sec,det.cor_matcat_n3_sec, det.cor_movlote_cantidad,det.cor_almacen_sec,lote.cor_factura_sec
					FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
					WHERE lote.cor_movimiento_sec = det.cor_movimiento_sec and det.cor_movimiento_sec = """+sec+";")	
		result2 = cur.fetchall()
		if (result2==None):
			print(json.dumps(myReturnData))
			#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
		else:
			for row in result2:
				cur.execute("SELECT cor_matcat_id FROM cor_inv_matcat WHERE cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
				result6=cur.fetchone()
				cur.execute("SELECT cor_matcat_n1_id FROM cor_inv_matcat_niv1 WHERE cor_matcat_n1_sec="+str(row[1])+";")
				result3=cur.fetchone()
				cur.execute("SELECT cor_matcat_n2_id FROM cor_inv_matcat_niv2 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+";")
				result4=cur.fetchone()
				cur.execute("SELECT cor_matcat_n3_id FROM cor_inv_matcat_niv3 WHERE cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+";")
				result5=cur.fetchone()
				cur.execute("SELECT cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_sec="+str(row[5])+";")
				result7=cur.fetchone()
				cur.execute("SELECT cor_detfactura_cantidad_pend FROM cor_inv_det_factura WHERE cor_factura_sec ="+ str(row[6])+" and cor_matcat_sec="+str(row[0])+" and cor_matcat_n1_sec="+str(row[1])+" and cor_matcat_n2_sec="+str(row[2])+" and cor_matcat_n3_sec="+str(row[3])+" ;")
				result8=cur.fetchone()
				myReturnData.append({"material":result6[0],"grupo":result3[0],"subgrupo":result4[0],"clase":result5[0],"unidades":row[4],"almacen":result7[0],"faltan":result8[0]})
	
	return json.dumps(myReturnData)

	
@route('/ConsultaLxCAll')
def ConsultaLxCAll():

	myReturnData=[]
	
	cur.execute("""SELECT lote_det.cor_matcat_sec, lote_det.cor_matcat_n1_sec,lote_det.cor_matcat_n2_sec,lote_det.cor_matcat_n3_sec, lote.cor_movimiento_id, lote.cor_movimiento_fecha, lote_det.cor_movlote_por_codificar  
				FROM cor_inv_movimiento_lote lote,cor_inv_movimiento_lote_detalle lote_det
				WHERE lote.cor_movimiento_sec=lote_det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus='P' ;""")
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
	
@route('/ConsultaLxC/<almacen>')
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

@route('/CFacTransAll')
def CFacTransAll():
	myReturnData=[]
	
	cur.execute("""SELECT cor_factura_nro,cor_factura_fec_emision,cor_factura_estatus,cor_factura_total_items,cor_factura_total_neto,cor_factura_sec
				 FROM cor_inv_factura
				 WHERE cor_factura_estatus != 'P';""")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			if(row[2]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"nro":row[0],"fecha":str(row[1]),"estatus":est,"items":row[3],"monto":str(row[4]),"sec":row[5]})
			
	return json.dumps(myReturnData)
	
@route('/CFacTrans/<FI>/<FF>')
def CFacTrans(FI,FF):
	myReturnData=[]
	
	cur.execute("""SELECT cor_factura_nro,cor_factura_fec_emision,cor_factura_estatus,cor_factura_total_items,cor_factura_total_neto,cor_factura_sec
				 FROM cor_inv_factura
				 WHERE cor_factura_estatus != 'P' and cor_factura_fec_emision between '"""+FI+" 00:00:00' and '"+FF+" 23:59:59';")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			if(row[2]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"nro":row[0],"fecha":str(row[1]),"estatus":est,"items":row[3],"monto":str(row[4]),"sec":row[5]})
			
	return json.dumps(myReturnData)	

@route('/ConsultaFTrans/<sec>')
def ConsultaFTrans(sec):
	myReturnData=[]
	
	cur = bdconn.cursor()
	cur.execute("""SELECT fac.cor_factura_nro,fac.cor_factura_fec_emision,fac.cor_factura_estatus,fac.cor_factura_total_items,fac.cor_factura_sec,fac.cor_empinstsec,fac.cor_factura_fec_recepcion,alm.cor_almacen_id
				 FROM cor_inv_factura fac,cor_inv_almacen alm
				 WHERE fac.cor_factura_sec="""+sec+" and fac.cor_factura_almacen = alm.cor_almacen_sec;")
	result = cur.fetchone()
	
	cur.execute("""SELECT det.cor_detfactura_cantidad,det.cor_detfactura_cantidad_pend
				 FROM cor_inv_det_factura det
				 WHERE det.cor_factura_sec="""+sec+" ;")
	result1 = cur.fetchone()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		if(result[2]=='S'):
			est='Sin Procesar'
		else:
			est='Recepcion Parcial'
		cur.execute("SELECT cor_empinst_id FROM cor_crm_empinst WHERE cor_empinstsec="+str(result[5])+";")
		result2=cur.fetchone()
		if(result1==None):
			cant=0
			cantp=0
		else:
			cant=result1[0]
			cantp=result1[1]
		myReturnData.append({"nro":result[0],"emision":str(result[1]),"estatus":est,"items":result[3],"sec":str(result[4]),"empinst":result2[0],"recepcion":str(result[6]),"cant":cant,"cantp":cantp,"almacen":row[7]})
	
	return json.dumps(myReturnData)	
	
@route('/CAllRecepByFac/<sec>')
def CAllRecepByFac(sec):	
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT lote.cor_movimiento_sec, lote.cor_movimiento_nro_recepcion, lote.cor_responsable_sec, lote.cor_movimiento_fecha, lote_det.cor_movlote_cantidad, lote.cor_movimiento_estatus
				   FROM cor_inv_movimiento_lote as lote,cor_inv_movimiento_lote_detalle as lote_det 
				   WHERE lote.cor_movimiento_sec =lote_det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus != 'P' and lote.cor_factura_sec="""+sec+";")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[2])+";")
			result1 = cur.fetchone()
			if(row[5]=='S'):
				est='Sin Procesar'
			else:
				est='Recepcion Parcial'
			myReturnData.append({"sec":row[0],"nro":row[1],"resp":result1[0],"fecha":str(row[3]),"cant":row[4],"estatus":est})

	return json.dumps(myReturnData)

@route('/PreIngresarRecep/<fac>/<mov>')
def PreIngresarRecep(fac,mov):
	myReturnData=[]
	
	cur.execute("""SELECT fac.cor_factura_nro, emp.cor_empinst_id,alm.cor_almacen_id, sum(det.cor_detfactura_cantidad_pend)
				   FROM cor_inv_factura fac,cor_inv_det_factura det,cor_crm_empinst emp, cor_inv_almacen alm
				   WHERE fac.cor_factura_sec = """+fac+""" and fac.cor_factura_sec = det.cor_factura_sec and fac.cor_empinstsec= emp.cor_empinstsec and fac.cor_factura_almacen = alm.cor_almacen_sec
				   GROUP BY fac.cor_factura_nro, emp.cor_empinst_id,alm.cor_almacen_id;""")
	result1=cur.fetchone()
	if (result1==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		myReturnData.append({"nro":result1[0],"empinst":result1[1],"almacen":result1[2],"items":result1[3]})
	if(int(mov)==0):
		cur.execute("SELECT cor_responsable_sec, cor_responsable_nombre FROM cor_crm_responsable")
		result = cur.fetchall()
		if (result==None):
			print(json.dumps(myReturnData))
			#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
		else:
			for row in result:
				myReturnData.append({"sec":row[0],"nombre":row[1]})		
		cur.close()
		bdconn.close()		
		return json.dumps(myReturnData)		
	else:
		cur.execute("""SELECT cor_movimiento_id,cor_movimiento_desc,cor_movimiento_fecha,cor_responsable_nombre,cor_movimiento_estatus,cor_movimiento_nro_recepcion
					  FROM cor_inv_movimiento_lote lote,cor_crm_responsable resp
					  WHERE lote.cor_movimiento_sec="""+mov+" and lote.cor_responsable_sec=resp.cor_responsable_sec;")
		result = cur.fetchone()
		if(result[4]=='S'):
			est='Sin Procesar'
		else:
			est='Recepcion Parcial'
		myReturnData.append({"id":result[0],"desc":result[1],"fecha":str(result[2]),"resp":result[3],"estatus":est,"nro":result[5]})

		return json.dumps(myReturnData)
		
@route('/IngresarRecep/')
def IngresarRecep():
	myReturnData=[]
	datos=dict(request.GET)
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	
	sql = "SELECT nextval('cor_inv_movimiento_lote_sec');"
	cur.execute(sql)
	sqlresult= cur.fetchone()
	insert="""INSERT INTO cor_inv_movimiento_lote(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_movimiento_tipo, cor_factura_sec, cor_responsable_sec, cor_movimiento_estatus, 
            cor_movimiento_nro_recepcion, cor_movimiento_fec_mod,cor_movimiento_ip_mod, cor_movimiento_login_mod)
    VALUES ("""+str(sqlresult[0])+",'"+datos["id"]+"','"+datos["des"]+"','"+fechah+"','R',"+datos["fac"]+","+datos["resp"]+",'S',"+datos["nro"]+"""
			,'"""+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+str(socket.gethostbyname(socket.gethostname()))+"','"+datos["user"]+"');"
	cur.execute(insert);
	myReturnData.append({"sec":sqlresult[0]})
	bdconn.commit()

	return json.dumps(myReturnData)	
	
	
@route('/t_cor_inv_movimiento_lote_0005_insertar_inventario_inicial')
def t_cor_inv_movimiento_lote_0005_insertar_inventario_inicial():
	myReturnData=[]
	datos=dict(request.GET)
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	
	sql = "SELECT nextval('cor_inv_movimiento_lote_sec');"
	cur.execute(sql)
	sqlresult= cur.fetchone()
	insert="""INSERT INTO cor_inv_movimiento_lote(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_movimiento_tipo, cor_responsable_sec, cor_movimiento_estatus, 
            cor_movimiento_fec_mod, cor_movimiento_ip_mod, cor_movimiento_login_mod)
    VALUES ("""+str(sqlresult[0])+",'"+datos["id"]+"','"+datos["des"]+"','"+fechah+"','R',"+datos["resp"]+",'S'"+"""
			,'"""+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+datos["alm"]+"','"+datos["user"]+"');"
	cur.execute(insert);
	myReturnData.append({"sec":sqlresult[0]})
	bdconn.commit()

	return json.dumps(myReturnData)	
	
@route('/ConsultaMatcatNew/<fac>')
def ConsultaMatcatNew(fac):
	myReturnData=[]
	
	cur.execute("""SELECT det.cor_matcat_sec,mat.cor_matcat_id,det.cor_detfactura_costo_unitario,det.cor_detfactura_cantidad_pend
				FROM cor_inv_det_factura det, cor_inv_matcat mat
				WHERE det.cor_factura_sec="""+fac+" and det.cor_matcat_sec=mat.cor_matcat_sec;")
	result=cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1],"monto":str(row[2]),"pend":row[3]})

	return json.dumps(myReturnData)	

@route('/IngresarRecepcionDet/<matcatsec>/<mov>/<almacen>/<cant>/<user>/<costo>')
def IngresarRecepcionDet(matcatsec,mov,almacen,cant,user,costo):
	myReturnData=[]
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_inv_movimiento_lote_detalle_sec');"
	cur.execute(sql)
	resultsql=cur.fetchone()
	matcat="""SELECT det.cor_matcat_n1_sec,det.cor_matcat_n2_sec,det.cor_matcat_n3_sec,det.cor_detfactura_sec,det.cor_detfactura_cantidad_pend,lote.cor_factura_sec
			  FROM cor_inv_det_factura det,cor_inv_movimiento_lote lote
			  WHERE det.cor_factura_sec=lote.cor_factura_sec and lote.cor_movimiento_sec="""+mov+"""
			  and det.cor_matcat_sec = """+matcatsec+" and det.cor_detfactura_costo_unitario = "+costo+";"
				
	cur.execute(matcat)
	resultm=cur.fetchone()
	pend=int(resultm[4])-int(cant)	
	det_fac_sec=str(resultm[3])
	fac=resultm[5]
	insert="""INSERT INTO cor_inv_movimiento_lote_detalle(
            cor_movlote_sec, cor_matcat_sec, cor_matcat_n3_sec, cor_matcat_n2_sec, 
            cor_matcat_n1_sec, cor_movimiento_sec, cor_almacen_sec, cor_movlote_cantidad, 
            cor_movlote_fec_mod, cor_movlote_ip_mod, cor_movlote_login_mod, 
            cor_movlote_codificados,cor_movlote_por_codificar, cor_movlote_costo_unitario)
    VALUES ("""+str(resultsql[0])+","+matcatsec+" ,"+str(resultm[2])+","+str(resultm[1])+""", 
            """+str(resultm[0])+","+mov+" ,"+almacen+" ,"+str(cant)+""", 
            '"""+fech_mod+"','"+ip_mod+"','"+user+"',0,"+cant+","+costo+" );"
	cur.execute(insert)
	myReturnData.append({"sec": resultsql[0]})
	
	update="""UPDATE cor_inv_det_factura
			  SET  cor_detfactura_cantidad_pend="""+str(pend)+""",
				   cor_detfactura_fec_mod='"""+fech_mod+"', cor_detfactura_ip_mod='"+ip_mod+"', cor_detfactura_login_mod='"+user+"""'
			 WHERE cor_detfactura_sec = """+det_fac_sec+";"
	cur.execute(update)	

	update1="""UPDATE cor_inv_movimiento_lote
			   SET cor_movimiento_estatus='R', cor_movimiento_fec_mod='"""+fech_mod+"', cor_movimiento_ip_mod='"+ip_mod+"""', 
			   cor_movimiento_login_mod='"""+user+"""'
			   WHERE cor_movimiento_sec ="""+mov+";"
	cur.execute(update1)
	
	update2="UPDATE cor_inv_factura SET cor_factura_estatus='R' WHERE cor_factura_sec="+str(fac)+";"
	cur.execute(update2)
	
	bdconn.commit()
	
	return json.dumps(myReturnData)	
	
@route('/CerrarRecep/<mov>/<user>')
def CerrarRecep(mov,user):
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	update1="""UPDATE cor_inv_movimiento_lote
			   SET cor_movimiento_estatus='P', cor_movimiento_fec_mod='"""+fech_mod+"', cor_movimiento_ip_mod='"+ip_mod+"""', 
			   cor_movimiento_login_mod='"""+user+"""'
			   WHERE cor_movimiento_sec ="""+mov+";"
	cur.execute(update1)
	bdconn.commit()

@route('/PreCodif')
def PreCodif():	
	myReturnData=[]
	myReturnData1=[]
		
	sql="""SELECT DISTINCT det.cor_matcat_sec,matcat.cor_matcat_id
	       FROM cor_inv_matcat matcat,cor_inv_movimiento_lote_detalle det,cor_inv_movimiento_lote lote
	       WHERE lote.cor_movimiento_sec=det.cor_movimiento_sec and lote.cor_movimiento_estatus='P' and 
	       det.cor_movlote_por_codificar > 0 and det.cor_matcat_sec = matcat.cor_matcat_sec and 
		   lote.cor_movimiento_tipo='R';"""
	cur.execute(sql)
	result=cur.fetchall()
	cur.execute("SELECT cor_almacen_sec,cor_almacen_id FROM cor_inv_almacen;")
	result1=cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
		myReturnData.append(myReturnData1)
	
	myReturnData1=[]
	
	if (result1==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result1:
			myReturnData1.append({"sec":row[0],"id":row[1]})
		myReturnData.append(myReturnData1)

	return json.dumps(myReturnData)	
	
@route('/CCodificacionAll')	
def CCodificacionAll():

	myReturnData = []
	
	cur.execute("""SELECT cor_movimiento_sec, cor_movimiento_id, cor_movimiento_fecha, cor_responsable_sec
				   FROM cor_inv_movimiento_lote
				   WHERE cor_movimiento_tipo='C';""")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[3])+";")
			result1 = cur.fetchone()
			myReturnData.append({"sec":row[0],"resp":result1[0],"fecha":str(row[2]),"id":row[1]})	
	return json.dumps(myReturnData)
				   
@route('/PreCodifMatcat/<matcat>')
def PreCodifMatcat(matcat):
	myReturnData=[]
	
	cur.execute("""SELECT sum(det.cor_movlote_por_codificar)
				   FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
				   WHERE det.cor_matcat_sec ="""+matcat+""" and det.cor_movimiento_sec=lote.cor_movimiento_sec and 
				   lote.cor_movimiento_tipo='R' and lote.cor_movimiento_estatus='P';""")
	result=cur.fetchone()
	myReturnData.append({"pend":result[0]})
	
	return json.dumps(myReturnData)	
	
@route('/ConsultaResp')
def ConsultaResp():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_responsable_sec,cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_activo='S';")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	
	return json.dumps(myReturnData)

@route('/IngresarCod')
def IngresarCod():
	myReturnData=[]
	datos=dict(request.GET)
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	
	sql = "SELECT nextval('cor_inv_movimiento_lote_sec');"
	cur.execute(sql)
	sqlresult= cur.fetchone()
	
	insert="""INSERT INTO cor_inv_movimiento_lote(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_movimiento_tipo, cor_responsable_sec, 
            cor_movimiento_fec_mod,cor_movimiento_ip_mod, cor_movimiento_login_mod)
    VALUES ("""+str(sqlresult[0])+",'"+datos["id"]+"','"+datos["desc"]+"','"+fechah+"','C',"+datos["resp"]+""",
			'"""+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+str(socket.gethostbyname(socket.gethostname()))+"','"+datos["user"]+"');"
	cur.execute(insert);
	myReturnData.append({"sec":sqlresult[0]})
	bdconn.commit()

	return json.dumps(myReturnData)

@route('/ListDetCod/<mov>')
def ListDetCod(mov):
	myReturnData=[]
	
	cur.execute("""SELECT matcat.cor_matcat_id,matcat.cor_matcat_sec,sum(det.cor_movlote_cantidad)
				   FROM cor_inv_movimiento_lote_detalle det, cor_inv_matcat matcat
				   WHERE det.cor_movimiento_sec="""+mov+""" and det.cor_matcat_sec=matcat.cor_matcat_sec  
				   GROUP BY matcat.cor_matcat_id,matcat.cor_matcat_sec;""")
	result=cur.fetchall()	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			sql="""SELECT sum(cor_movlote_por_codificar)
				   FROM cor_inv_movimiento_lote_detalle det,cor_inv_movimiento_lote lote
				   WHERE lote.cor_movimiento_sec=det.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and
				   det.cor_matcat_sec ="""+str(row[1])+";"
			cur.execute(sql)
			resultp=cur.fetchone()
			myReturnData.append({"material":row[0],"cod":row[2],"por":resultp[0]})
	
	return json.dumps(myReturnData)	

@route('/ValidarCod/')
def ValidarCod():
	myReturnData=[]
	myReturnData1=[]
	datos=dict(request.GET)
	
	if(datos["CF"]==""):
		CF=datos["CI"]
	else:
		CF=datos["CF"]
		
	cur.execute("SELECT cor_articulo_sec FROM cor_inv_articulo;")
	result=cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			if(row[0]>=datos["CI"] and row[0]<=CF):
				MyReturnData.append({"sec":row[0]})

	return json.dumps(myReturnData)			
		
@route('/GenerarCod/')
def GenerarCod():
	myReturnData=[]
	datos=dict(request.GET)
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	if(datos["CF"]==""):
		cant=1
	else:
		cant=abs(int(datos["CF"])-int(datos["CI"]))+1
		
	sql = "SELECT nextval('cor_inv_movimiento_lote_detalle_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()

	cur.execute("SELECT cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec FROM cor_inv_matcat WHERE cor_matcat_sec="+datos["matcat"]+";")
	result=cur.fetchone()
	insertini="""INSERT INTO cor_inv_articulo(
            cor_articulo_sec,cor_articulo_desc,cor_articulo_serial,cor_almacen_sec, cor_matcat_sec, cor_matcat_n3_sec, cor_matcat_n2_sec, cor_matcat_n1_sec, 
			cor_articulo_compuesto, cor_articulo_fec_mod, cor_articulo_ip_mod, cor_articulo_login_mod, cor_articulo_estatus)
			VALUES ("""
	insertfin=",'','',"+datos["almacen"]+","+datos["matcat"]+","+str(result[2])+","+str(result[1])+","+str(result[0])+",'N','"+fech_mod+"','"+ip_mod+"','"+datos["user"]+"', 138);"
	if(datos["CF"]!=""):
		i=int(datos["CI"]);
		while i<=int(datos["CF"]):
			cur.execute(insertini+""+str(i)+""+insertfin)
			i+=1
	else:
		cur.execute(insertini+""+datos["CI"]+""+insertfin)
		
	#Coddet 
	cur.execute("""INSERT INTO cor_inv_movimiento_lote_detalle(
            cor_movlote_sec, cor_matcat_sec, cor_matcat_n3_sec, cor_matcat_n2_sec, 
            cor_matcat_n1_sec, cor_movimiento_sec, cor_almacen_sec, cor_movlote_cantidad, 
            cor_movlote_fec_mod, cor_movlote_ip_mod, cor_movlote_login_mod, 
            cor_movlote_campo1, cor_movlote_campo2)
    VALUES ("""+str(sqlresult[0])+","+datos["matcat"]+","+str(result[2])+" ,"+str(result[1])+""" , 
            """+str(result[0])+","+datos["mov"]+","+datos["almacen"]+" ,"+str(cant)+""",' 
            """+fech_mod+"','"+ip_mod+"','"+datos["user"]+"',"+datos["CI"]+","+datos["CF"]+");")		
		
	#RECEPCIONES
	consult="""SELECT det.cor_movlote_sec,det.cor_movlote_codificados, det.cor_movlote_por_codificar,det.cor_movlote_cantidad
			   FROM cor_inv_movimiento_lote_detalle det, cor_inv_movimiento_lote lote
			   WHERE det.cor_movimiento_sec=lote.cor_movimiento_sec and lote.cor_movimiento_tipo='R' and
			   det.cor_matcat_sec="""+datos["matcat"]+" and lote.cor_movimiento_estatus='P' and det.cor_movlote_por_codificar > 0;"
	cur.execute(consult)
	recep=cur.fetchall()
	if (recep==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		i=int(datos["CI"])
		ii=0
		while cant>0:
			if(cant==1):
				fin=0
			else:
				fin=cant+i-1
			if(recep[ii][2]<cant): 
				update="""UPDATE cor_inv_movimiento_lote_detalle
					      SET cor_almacen_sec='"""+datos["almacen"]+"', cor_movlote_fec_mod='"+fech_mod+"', cor_movlote_ip_mod='"+ip_mod+"""', 
					      cor_movlote_login_mod='"""+datos["user"]+"""', 
					      cor_movlote_codificados="""+str(recep[ii][3])+""", cor_movlote_por_codificar=0
					      WHERE cor_movlote_sec="""+str(recep[ii][0])+";"
				cur.execute(update)
				cant-=recep[ii][2]
				i=fin+1
				ii+=1
			else:
				cod=recep[ii][1]+cant
				por=recep[ii][2]-cant
				update="""UPDATE cor_inv_movimiento_lote_detalle
					      SET cor_almacen_sec='"""+datos["almacen"]+"', cor_movlote_fec_mod='"+fech_mod+"', cor_movlote_ip_mod='"+ip_mod+"""', 
					      cor_movlote_login_mod='"""+datos["user"]+"""', 
					      cor_movlote_codificados="""+str(cod)+", cor_movlote_por_codificar="+str(por)+"""
					      WHERE cor_movlote_sec="""+str(recep[ii][0])+";"
				cur.execute(update)
				cant=0

	bdconn.commit()
	
@route('/CCodificacion/<FI>/<FF>')
def CCodificacion(FI,FF):	
	myReturnData=[]
	
	consulta="""SELECT lote.cor_movimiento_sec, lote.cor_responsable_sec, lote.cor_movimiento_fecha,lote.cor_movimiento_id
			    FROM cor_inv_movimiento_lote as lote
			    WHERE lote.cor_movimiento_tipo='C' and lote.cor_movimiento_fecha between '"""+FI+" 00:00:00' and '"+FF+""" 23:59:59';"""
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute(consulta)
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[1])+";")
			result1 = cur.fetchone()
			myReturnData.append({"sec":row[0],"resp":result1[0],"fecha":str(row[2]),"id":row[3]})	
	return json.dumps(myReturnData)
	
@route('/ConsultaCodif/<sec>')
def ConsultaCodif(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec
				   FROM cor_inv_movimiento_lote as lote
				   WHERE lote.cor_movimiento_sec =	"""+sec+";""")
	result = cur.fetchone()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		cur.execute("SELECT sum(cor_movlote_cantidad) FROM cor_inv_movimiento_lote_detalle WHERE cor_movimiento_sec="+sec+";") 
		resultdet=cur.fetchone()
		if(resultdet[0] == None ):
			nro=0;
		else:
			nro=resultdet[0]
		cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(result[3])+";")
		result1 = cur.fetchone()
		myReturnData.append({"id":result[0],"desc":result[1],"fecha":str(result[2]),"resp":result1[0],"cant":nro})
		
	return json.dumps(myReturnData)
	
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

@route('/PreDespacho/<comp>/<mov>')
def PreDespacho(comp,mov):
	myReturnData=[]

	cur.execute("SELECT cor_param_texto30 FROM cor_base_param WHERE cor_param_id='MODO_CODIFICACION';")
	myReturnData.append({"param":cur.fetchone()[0]})
	
	cur.execute("""SELECT matcat.cor_matcat_sec,matcat.cor_matcat_id, comp.cor_composicion_cantidad 
				   FROM cor_inv_matcat matcat,cor_inv_composicion_detalle comp
				   WHERE matcat.cor_matcat_sec=comp.cor_matcat_sec and comp.cor_composicion_sec="""+comp+";")
	matcat=cur.fetchall()
	if (matcat==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in matcat:
			cur.execute("SELECT COUNT(*) FROM cor_inv_movimiento_detalle WHERE cor_movimiento_sec="+mov+" and cor_matcat_sec="+str(row[0])+";")
			desp=cur.fetchone()
			myReturnData.append({"sec":row[0],"id":row[1],"cant":row[2],"desp":desp[0]})
	
	return json.dumps(myReturnData)
	
def validarcompuesto(sec):

	cur.execute("SELECT cor_articulo_componente_sec FROM cor_inv_articulo_componente WHERE cor_articulo_compuesto_sec="+str(sec)+";")
	result=cur.fetchall()
	if(result==None):
		return True
	else:
		for row in result:
			cur.execute("SELECT cor_articulo_compuesto,cor_articulo_estatus FROM cor_inv_articulo WHERE cor_articulo_sec="+str(row[0])+";")
			result0=cur.fetchone()
			if(result0[1]==138):
				if(result0[0]=="S" and validarcompuesto(row[0])):
					return True
			else:
				return True
	return False	

#VERIFICAR	
@route('/ValidarDespacho/')
def ValidarDespacho():
	
	datos=dict(request.GET)
	matcat="["+datos["matcat"]+"]"
	myReturnData = []
	myReturnData1 = []
	matcats=eval(matcat)
	ini=int(datos["CI"])
	if(datos["CF"]!=""):
		fin=int(datos["CF"])
	else:
		fin=int(datos["CI"])
	while ini<=fin:
		cur.execute("SELECT cor_matcat_sec,cor_articulo_compuesto FROM cor_inv_articulo WHERE cor_articulo_sec="+str(ini)+" and cor_articulo_estatus=138;")
		result=cur.fetchone()
		ini=int(ini)
		if(result==None):
			myReturnData.append({"sec":ini})
		elif(matcats.count(result[0])==0):
			myReturnData.append({"sec":ini})
		elif(result[1]=="S"):
			if(validarcompuesto(ini)):
				myReturnData1.append({"sec":ini})
		ini+=1
	myReturnData.insert(0,myReturnData1)	
	return json.dumps(myReturnData)
		
def auxdespachar(mov,row,user):
	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	cur.execute("SELECT cor_matcat_sec,cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec,cor_articulo_compuesto FROM cor_inv_articulo WHERE cor_articulo_sec="+str(row)+";")
	result1=cur.fetchone()
	
	update="""UPDATE cor_inv_articulo
			   SET cor_articulo_fec_mod='"""+fech_mod+"', cor_articulo_ip_mod='"+ip_mod+"""', 
				   cor_articulo_login_mod='"""+user+"""', cor_articulo_estatus=139
			 WHERE cor_articulo_sec="""+str(row)+";"
	cur.execute(update)
	if(result1[4]=="S"):
		despacharcompuesto(mov,row,user)
	
	sql = "SELECT nextval('cor_inv_movimiento_detalle_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()

	insert="""INSERT INTO cor_inv_movimiento_detalle(
			cor_movimiento_sec, cor_articulo_sec, cor_movdetalle_fec_mod, 
			cor_movdetalle_ip_mod, cor_movdetalle_login_mod, cor_matcat_sec, 
			cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cor_movdetalle_sec)
	VALUES ("""+mov+","+str(row)+" ,'"+fech_mod+"""', 
			'"""+ip_mod+"','"+user+"',"+str(result1[0])+""" , 
			"""+str(result1[1])+","+str(result1[2])+" , "+str(result1[3])+","+str(sqlresult[0])+");"""
	cur.execute(insert)	
	
def despacharcompuesto(mov,sec,user):
	
	cur.execute("SELECT cor_articulo_componente_sec FROM cor_inv_articulo_componente WHERE cor_articulo_compuesto_sec="+str(sec)+";")
	result=cur.fetchall()
	for row in result:
		auxdespachar(mov,row[0],user)
		
@route('/Despacho/')
def Despacho():

	datos=dict(request.GET)
	ini=datos["CI"]
	if(datos["CF"]!=""):
		fin=datos["CF"]
	else:
		fin=datos["CI"]
	while ini<=fin:
		auxdespachar(datos["mov"],ini,datos["user"])
		ini+=1
	
	bdconn.commit()	
	
@route('/CerrarDespacho/<mov>/<user>')	
def CerrarDespacho(mov,user):

	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	update="""UPDATE cor_inv_movimiento
			 SET cor_movimiento_fec_mod='"""+fech_mod+"', cor_movimiento_ip_mod='"+ip_mod+"', cor_movimiento_login_mod='"+user+"""',
			 cor_movimiento_estatus='C' WHERE cor_movimiento_sec="""+mov+"; """
	cur.execute(update)
	bdconn.commit()
			
@route('/PreNewDespacho/')
def PreNewDespacho():
	
	myReturnData=[]
	myReturnData1=[]
	datos=dict(request.GET)
	
	#Responsable
	cur.execute("SELECT cor_responsable_sec,cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_activo='S';")
	result=cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
		myReturnData.append(myReturnData1)
		
	#Empinst
	cur.execute("SELECT cor_empinstsec,cor_empinst_id FROM cor_crm_empinst cor_empinst_activo='S';")
	result=cur.fetchall()
	myReturnData1=[]
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
		myReturnData.append(myReturnData1)
		
	#Contacto
	myReturnData1=[]		
	if(datos["emp"]!=""):
		cur.execute("SELECT cor_contactosec,cor_contacto_nombre FROM cor_crm_contacto WHERE cor_empinstsec="+datos["emp"]+" and cor_contacto_activo='S';")
		result=cur.fetchall()
		if (result==None):
			print(json.dumps(myReturnData))
			#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
		else:
			for row in result:
				myReturnData1.append({"sec":row[0],"id":row[1]})
	myReturnData.append(myReturnData1)

	#Composicion
	myReturnData1=[]
	cur.execute("SELECT cor_composicion_sec,cor_composicion_id FROM cor_inv_composicion WHERE cor_composicion_activo='S';")
	result=cur.fetchall()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
		myReturnData.append(myReturnData1)
	
	return json.dumps(myReturnData)
	
@route('/NewDespacho/')
def NewDespacho():
	
	myReturnData = []
	datos=dict(request.GET)
	
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_inv_movimiento_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	insert="""INSERT INTO cor_inv_movimiento(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_responsable_sec, cor_contactosec, 
            cor_empinstsec,  cor_movimiento_fec_mod, 
            cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_tipo, 
            cor_movimiento_compsicion, cor_movimiento_cantidad,cor_movimiento_estatus)
    VALUES ( """+str(sqlresult[0])+",'"+datos["id"]+"', '"+datos["desc"]+"', '"+fechah+"', "+datos["resp"]+""", 
            """+datos["contacto"]+","+datos["emp"]+", '"+fech_mod+"','"+ip_mod+"','"+datos["user"]+"""','D' , 
            """+datos["comp"]+","+datos["cant"]+",'A');"""
	cur.execute(insert)
	myReturnData.append({"mov":sqlresult[0]})
	
	bdconn.commit()

	return json.dumps(myReturnData)
	
@route('/CDespacho/<FI>/<FF>')
def CDespacho(FI,FF):	
	myReturnData=[]
	
	consulta="""SELECT cor_movimiento_sec, cor_responsable_sec, cor_movimiento_fecha,cor_movimiento_id
			    FROM cor_inv_movimiento 
			    WHERE cor_movimiento_tipo='D' and cor_movimiento_estatus ='A' and cor_movimiento_fecha between '"""+FI+" 00:00:00' and '"+FF+""" 23:59:59';"""
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute(consulta)
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[1])+";")
			result1 = cur.fetchone()
			myReturnData.append({"sec":row[0],"resp":result1[0],"fecha":str(row[2]),"id":row[3]})
	
	return json.dumps(myReturnData)
	
@route('/ArtConsultaComp/<sec>')
def ArtConsultaComp(sec):
	
	myReturnData = []
	cur.execute("""SELECT art.cor_articulo_sec,art.cor_articulo_id,alm.cor_almacen_id,mat.cor_matcat_id,art.cor_articulo_estatus
				   FROM cor_inv_articulo art, cor_inv_almacen alm, cor_inv_matcat mat, cor_inv_articulo_componente artc
				   WHERE artc.cor_articulo_compuesto_sec="""+sec+" and artc.cor_articulo_componente_sec=art.cor_articulo_sec and art.cor_almacen_sec=alm.cor_almacen_sec and art.cor_matcat_sec=mat.cor_matcat_sec;")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			if(row[4]==138):
				est="En Almacen"
			elif(row[4]==139):
				est="Despachado"
			elif(row[4]==140):
				est="Mermado"				
			myReturnData.append({"sec":row[0],"id":row[1],"alm_id":row[2],"matcat_id":row[3],"estatus":est})
	return json.dumps(myReturnData)	
		
@route('/ArtConsulta/<sec>')
def ArtConsulta(sec):

	myReturnData = []
	myReturnData1 = []
	cur.execute("""SELECT art.cor_articulo_id,art.cor_articulo_desc,alm.cor_almacen_id,mat.cor_matcat_id,art.cor_articulo_compuesto,art.cor_articulo_estatus,art.cor_articulo_serial,art.cor_color_sec,art.cor_matprima_sec,col.cor_color_id,matp.cor_matprima_id
				   FROM cor_inv_articulo art,cor_inv_almacen alm,cor_inv_matcat mat,cor_inv_color col,cor_inv_materia_prima matp
				   WHERE art.cor_articulo_sec="""+sec+" and alm.cor_almacen_sec=art.cor_almacen_sec and mat.cor_matcat_sec=art.cor_matcat_sec and art.cor_color_sec=col.cor_color_sec and art.cor_matprima_sec=matp.cor_matprima_sec;")
	result=cur.fetchone()
	if(result!=None):
		if(result[5]==138):
			est="En Almacen"
		elif(result[5]==139):
			est="Despachado"
		elif(result[5]==140):
			est="Mermado"
			
		myReturnData.append({"id":result[0],"desc":result[1],"alm_id":result[2],"matcat_id":result[3],"comp":result[4],"estatus":est,"serial":result[6],"color":result[7],"matprima":result[8],"colorid":result[9],"matprimaid":result[10]})
		
		if(est!="Despachado"):
			cur.execute("SELECT cor_matprima_sec,cor_matprima_id FROM cor_inv_materia_prima;")
			matprima=cur.fetchall()
			for row in matprima:
				myReturnData1.append({"sec":row[0],"id":row[1]})
			myReturnData.append(myReturnData1)
			
			myReturnData1= []
			
			cur.execute("SELECT cor_color_sec,cor_color_id FROM cor_inv_color;")
			matprima=cur.fetchall()
			for row in matprima:
				myReturnData1.append({"sec":row[0],"id":row[1]})
			myReturnData.append(myReturnData1)	

	return json.dumps(myReturnData)

@route('/ActualizarArt/')
def ActualizarArt():

	datos=dict(request.GET)
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	update="""UPDATE cor_inv_articulo
			  SET cor_articulo_desc='"""+datos["desc"]+"', cor_articulo_serial='"+datos["serial"]+"', cor_color_sec="+datos["color"]+", cor_matprima_sec="+datos["matprima"]+""",
				  cor_articulo_compuesto='"""+datos["comp"]+"', cor_articulo_fec_mod='"+fech_mod+"', cor_articulo_ip_mod='"+ip_mod+"""', 
				  cor_articulo_login_mod='"""+datos["user"]+"""'
			  WHERE cor_articulo_sec = """+datos["sec"]+";"
	cur.execute(update)
	if(datos["comp"]=="N"):
		cur.execute("DELETE FROM cor_inv_articulo_componente WHERE cor_articulo_compuesto_sec="+datos["sec"]+";")
	bdconn.commit()
	
@route('/AgregarComp/<art>/<comp>')
def AgregarComp(art,comp):

	myReturnData = []
	
	cur.execute("SELECT * FROM cor_inv_articulo WHERE cor_articulo_sec="+comp+";")
	result=cur.fetchone()
	if (result==None):
		myReturnData.append({"error":"S"})
	else:
		cur.execute("""INSERT INTO cor_inv_articulo_componente(
					   cor_articulo_compuesto_sec, cor_articulo_componente_sec)
					   VALUES ("""+art+", "+comp+");")
		myReturnData.append({"error":"N"})
		bdconn.commit()
	
	return json.dumps(myReturnData)	
	
@route('/NewInventario/')
def NewInventario():
	myReturnData = []
	datos=dict(request.GET)
	
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_inv_movimiento_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	insert="""INSERT INTO cor_inv_movimiento(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_responsable_sec,cor_almacen_sec, cor_movimiento_fec_mod, 
            cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_tipo,cor_movimiento_estatus)
    VALUES ( """+str(sqlresult[0])+",'"+datos["id"]+"', '"+datos["desc"]+"', '"+fechah+"', "+datos["resp"]+""",
			"""+datos["almacen"]+",'"+fech_mod+"','"+ip_mod+"','"+datos["user"]+"','I','A');"
	cur.execute(insert)
	myReturnData.append({"mov":sqlresult[0]})
	
	bdconn.commit()

	return json.dumps(myReturnData)
	
@route('/RegInventario/')
def RegInventario():

	myReturnData = []
	datos=dict(request.GET)	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	cur.execute("SELECT cor_matcat_n1_sec,cor_matcat_n2_sec,cor_matcat_n3_sec FROM cor_inv_matcat WHERE cor_matcat_sec="+str(datos["matcat"])+";")
	result=cur.fetchone()
	
	sql = "SELECT nextval('cor_inv_movimiento_detalle_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	insert=""" INSERT INTO cor_inv_movimiento_detalle(
            cor_movimiento_sec, cor_movdetalle_fec_mod, 
            cor_movdetalle_ip_mod, cor_movdetalle_login_mod, cor_matcat_sec, 
            cor_matcat_n1_sec, cor_matcat_n2_sec, cor_matcat_n3_sec, cantidad, 
            cor_movdetalle_sec)
    VALUES (""" +datos["mov"]+",'"+fech_mod+"' ,'"+ip_mod+"""' , 
            '"""+datos["user"]+"' ,"+datos["matcat"]+" ,"+str(result[0])+""" , 
            """+str(result[1])+", "+str(result[2])+", "+datos["cant"]+","+str(sqlresult[0])+");"
	cur.execute(insert)
	
	bdconn.commit()	

@route('/PreNewTraslado/')
def PreNewTraslado():

	myReturnData = []
	myReturnData1 = []
	
	cur.execute("SELECT cor_responsable_sec,cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_activo='S';")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
	
	myReturnData.append(myReturnData1)
	myReturnData1 = []
	
	cur.execute("SELECT cor_almacen_sec,cor_almacen_id FROM cor_inv_almacen WHERE cor_almacen_activo='S';")
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData1.append({"sec":row[0],"id":row[1]})
	
	myReturnData.append(myReturnData1)
	myReturnData1 = []

	cur.execute("SELECT cor_motivo_sec,cor_motivo_id FROM cor_inv_motivo_movimiento WHERE cor_motivo_activo='S';")
	result=cur.fetchone()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		myReturnData1.append({"sec":result[0],"id":result[1]})
		
	myReturnData.append(myReturnData1)
	
	return json.dumps(myReturnData)
	
@route('/NewTraslado/')
def NewTraslado():
	
	myReturnData = []
	datos=dict(request.GET)
	
	fechah=datos["fecha"]+" "+str(datetime.datetime.today().strftime("%H:%M:%S"))
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_inv_movimiento_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	insert="""INSERT INTO cor_inv_movimiento(
            cor_movimiento_sec, cor_movimiento_id, cor_movimiento_desc, cor_movimiento_fecha, 
            cor_motivo_sec,cor_responsable_sec, cor_movimiento_fec_mod, 
            cor_movimiento_ip_mod, cor_movimiento_login_mod, cor_movimiento_tipo,cor_movimiento_estatus)
    VALUES ( """+str(sqlresult[0])+",'"+datos["id"]+"', '"+datos["desc"]+"','"+fechah+"',"+datos["motivo"]+","+datos["resp"]+""",' 
            """+fech_mod+"','"+ip_mod+"','"+datos["user"]+"','T','A');"
	cur.execute(insert)
	myReturnData.append({"mov":sqlresult[0]})
	
	bdconn.commit()

	return json.dumps(myReturnData)
	
@route('/GetParam')
def GetParam():

	myReturnData = []
	
	cur.execute("SELECT cor_param_texto30 FROM cor_base_param WHERE cor_param_id='MODO_CODIFICACION';")
	myReturnData.append({"param":cur.fetchone()[0]})
	
	return json.dumps(myReturnData)
	
@route('/ValidarTras/')
def ValidarTras():

	myReturnData = []
	datos=dict(request.GET)
	ini=int(datos["CI"])
	while (ini<=int(datos["CF"])):
		cur.execute("SELECT cor_articulo_sec FROM cor_inv_articulo WHERE cor_articulo_sec="+str(ini)+" and cor_articulo_estatus=138;")
		result=cur.fetchone()
		if(result == None):
			myReturnData.append({"sec":ini})
		ini+=1
	return json.dumps(myReturnData)	
	
@route('/Trasladar/')	
def Trasladar():

	myReturnData = []
	datos=dict(request.GET)
	ini=int(datos["CI"])
	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	while (ini<=int(datos["CF"])):
		
		sql = "SELECT nextval('cor_inv_movimiento_detalle_sec');"
		cur.execute(sql)
		sqlresult=cur.fetchone()
		
		insert=""" INSERT INTO cor_inv_movimiento_detalle(
				cor_movimiento_sec, cor_articulo_sec ,cor_movdetalle_fec_mod, 
				cor_movdetalle_ip_mod, cor_movdetalle_login_mod,cor_movdetalle_sec)
		VALUES (""" +datos["mov"]+","+str(ini)+",'"+fech_mod+"' ,'"+ip_mod+"""' , 
				'"""+datos["user"]+"',"+str(sqlresult[0])+");"
		cur.execute(insert)
		
		cur.execute("""UPDATE cor_inv_articulo
					   SET cor_almacen_sec="""+datos["almacen"]+", cor_articulo_fec_mod='"+fech_mod+"""', 
					   cor_articulo_ip_mod='"""+ip_mod+"', cor_articulo_login_mod='"+datos["user"]+"""'
					   WHERE cor_articulo_sec="""+str(ini)+";")
		ini+=1
	
	bdconn.commit()	
	
@route('/NewAlmacen')
def NewAlmacen():

	myReturnData = []
	datos=dict(request.GET)
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_inv_almacen_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	insert="""INSERT INTO cor_inv_almacen(
            cor_almacen_sec, cor_almacen_id, cor_almacen_desc, cor_almacen_tipo, 
            cor_responsable_sec, cor_almacen_activo, cor_almacen_fec_mod, 
            cor_almacen_ip_mod, cor_almacen_login_mod)
    VALUES ("""+str(sqlresult[0]) +",'"+ datos["id"]+ "','"+ datos["desc"] +"','" +datos["tipo"]+"""', 
            """+datos["resp"]+" ,'"+ datos["activo"]+"' ,'"+fech_mod+"""' , 
            '"""+ip_mod+"' , '"+ datos["user"]+"'); """
	cur.execute(insert)
	myReturnData.append({"almacen":sqlresult[0]})
	
	bdconn.commit()
	return json.dumps(myReturnData)
	
@route('/CMovimiento/<FI>/<FF>/<tipo>')
def CMovimiento(FI,FF,tipo):	
	myReturnData=[]
	
	consulta="""SELECT cor_movimiento_sec, cor_responsable_sec, cor_movimiento_fecha,cor_movimiento_id
			    FROM cor_inv_movimiento 
			    WHERE cor_movimiento_tipo='"""+tipo+"' and cor_movimiento_fecha between '"+FI+" 00:00:00' and '"+FF+" 23:59:59';"
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute(consulta)
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[1])+";")
			result1 = cur.fetchone()
			myReturnData.append({"sec":row[0],"resp":result1[0],"fecha":str(row[2]),"id":row[3]})
	
	return json.dumps(myReturnData)
	
@route('/CMovimientoAll/<tipo>')
def CMovimientoAll(tipo):
	myReturnData=[]
	
	consulta="""SELECT cor_movimiento_sec, cor_responsable_sec, cor_movimiento_fecha,cor_movimiento_id
			    FROM cor_inv_movimiento 
			    WHERE cor_movimiento_tipo='"""+tipo+"' ;"
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute(consulta)
	result = cur.fetchall()
	
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			cur.execute("SELECT cor_responsable_nombre FROM cor_crm_responsable WHERE cor_responsable_sec ="+ str(row[1])+";")
			result1 = cur.fetchone()
			myReturnData.append({"sec":row[0],"resp":result1[0],"fecha":str(row[2]),"id":row[3]})
	
	return json.dumps(myReturnData)
	
@route('/ConsultaInventario/<sec>')
def ConsultaInventario(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec,lote.cor_almacen_sec,lote.cor_movimiento_estatus 
				   FROM cor_inv_movimiento as lote
				   WHERE lote.cor_movimiento_sec =	"""+sec+";")
	result = cur.fetchone()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		cur.execute("SELECT resp.cor_responsable_nombre,alm.cor_almacen_id FROM cor_crm_responsable resp,cor_inv_almacen alm WHERE resp.cor_responsable_sec ="+ str(result[3])+" and alm.cor_almacen_sec="+str(result[4])+";")
		result1 = cur.fetchone()
		myReturnData.append({"id":result[0],"desc":result[1],"fecha":str(result[2]),"resp":result1[0],"alm":result1[1],"estatus":result[5]})

	return json.dumps(myReturnData)
		
@route('/ConsultaTraslado/<sec>')
def ConsultaTraslado(sec):
	myReturnData=[]
	
	cur.execute("""SELECT lote.cor_movimiento_id,lote.cor_movimiento_desc,lote.cor_movimiento_fecha,lote.cor_responsable_sec,lote.cor_motivo_sec,lote.cor_movimiento_estatus 
				   FROM cor_inv_movimiento as lote
				   WHERE lote.cor_movimiento_sec =	"""+sec+";")
	result = cur.fetchone()
	if (result==None):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		cur.execute("SELECT resp.cor_responsable_nombre,mot.cor_motivo_id FROM cor_crm_responsable resp,cor_inv_motivo_movimiento mot WHERE resp.cor_responsable_sec ="+ str(result[3])+" and mot.cor_motivo_sec="+str(result[4])+";")
		result1 = cur.fetchone()
		myReturnData.append({"id":result[0],"desc":result[1],"fecha":str(result[2]),"resp":result1[0],"motivo":result1[1],"estatus":result[5]})

	return json.dumps(myReturnData)	

@route('/ConsultaZona/<alm>/<tipo>')
def ConsultaZona(alm,tipo):
	
	myReturnData = []
	if (tipo=='almacen'):
		consulta05='cor_inv_almacen_zona'
		consulta15='almacen.cor_almacen_sec='
	else:
		consulta05='cor_crm_responsable_zona'
		consulta15='almacen.cor_responsable_sec='
		
	consulta0= "SELECT z4.cor_zona_n4_id,z3.cor_zona_n3_id,z2.cor_zona_n2_id,z1.cor_zona_n1_id FROM " 
	consulta1=" almacen, cor_crm_zona_niv4 z4, cor_crm_zona_niv3 z3, cor_crm_zona_niv2 z2, cor_crm_zona_niv1 z1 WHERE "
	consulta2=""+alm+""" and almacen.cor_zona_n4_sec=z4.cor_zona_n4_sec
				   and almacen.cor_zona_n3_sec=z3.cor_zona_n3_sec and almacen.cor_zona_n2_sec=z2.cor_zona_n2_sec
				   and almacen.cor_zona_n1_sec=z1.cor_zona_n1_sec;"""
	consulta0+=consulta05+""+consulta1+""+consulta15+""+consulta2
	cur.execute(consulta0)
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"z4":row[0],"z3":row[1],"z2":row[2],"z1":row[3]})
	return json.dumps(myReturnData)	
	
@route('/NewZonaAlmacenz1')
def NewZonaAlmacenz1():

	myReturnData = []
	
	cur.execute("SELECT cor_zona_n1_sec,cor_zona_n1_id FROM cor_crm_zona_niv1 WHERE cor_zona_n1_activo='S';")
	result=cur.fetchall()
	
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)	

@route('/NewZonaAlmacenz2/<pais>')
def NewZonaAlmacenz2(pais):

	myReturnData = []
	
	cur.execute("SELECT cor_zona_n2_sec,cor_zona_n2_id FROM cor_crm_zona_niv2 WHERE cor_zona_n2_activo='S' and cor_zona_n1_sec="+pais+";")
	result=cur.fetchall()
	
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)		
	
@route('/NewZonaAlmacenz3/<pais>/<est>')
def NewZonaAlmacenz3(pais,est):

	myReturnData = []
	
	cur.execute("SELECT cor_zona_n3_sec,cor_zona_n3_id FROM cor_crm_zona_niv3 WHERE cor_zona_n3_activo='S' and cor_zona_n1_sec="+pais+" and cor_zona_n2_sec="+est+";")
	result=cur.fetchall()
	
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)		

@route('/NewZonaAlmacenz4/<pais>/<est>/<sector>')
def NewZonaAlmacenz4(pais,est,sector):

	myReturnData = []
	
	cur.execute("SELECT cor_zona_n4_sec,cor_zona_n4_id FROM cor_crm_zona_niv4 WHERE cor_zona_n4_activo='S' and cor_zona_n1_sec="+pais+" and cor_zona_n2_sec="+est+" and cor_zona_n3_sec="+sector+";")
	result=cur.fetchall()
	
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)		

@route('/ZonaTipo')
def ZonaTipo():

	myReturnData = []
	
	cur.execute("SELECT cor_tipo_zona_sec,cor_tipo_zona_id FROM cor_crm_tipo_zona WHERE cor_tipo_zona_activo='S';")
	result=cur.fetchall()
	
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)	
	
@route('/AgregarZona')	
def AgregarZona():
	
	myReturnData = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	datos=dict(request.GET)
	
	
	if(datos["AorR"]=="Almacen"):
		sql = "SELECT nextval('cor_inv_almacen_zona_sec');"
		cur.execute(sql)
		sqlresult=cur.fetchone()
		insert=""" INSERT INTO cor_inv_almacen_zona(
            cor_almacen_sec, cor_zona_n4_sec, cor_zona_n3_sec, cor_zona_n2_sec, 
            cor_zona_n1_sec, cor_almacen_zona_sec, cor_almacen_zona_desc, 
            cor_tipo_zona_sec, cor_almacen_zona_activo, cor_almacen_zona_fec_mod, 
            cor_almacen_zona_ip_md, cor_almacen_zona_login_mod)
			VALUES ("""+datos["alm"]+" , "+datos["zona"]+","+datos["sector"]+", "+datos["est"]+""", 
            """+datos["pais"]+", "+str(sqlresult[0])+", '"+datos["desc"]+"""', 
            """+datos["tipo"]+ ", 'S', '"+fech_mod+"""', 
            '"""+ip_mod+ "', '"+datos["user"]+"');"
	else:
		sql = "SELECT nextval('cor_crm_responsable_zona_sec');"
		cur.execute(sql)
		sqlresult=cur.fetchone()
		insert=""" INSERT INTO cor_crm_responsable_zona(
            cor_responsable_sec, cor_zona_n4_sec, cor_zona_n3_sec, cor_zona_n2_sec, 
            cor_zona_n1_sec, cor_responsable_zona_sec, cor_responsable_zona_desc, 
            cor_tipo_zona_sec, cor_responsable_zona_activo, cor_responsable_zona_fec_mod, 
            cor_responsable_zona_ip_mod, cor_responsable_zona_login_mod)
			VALUES ("""+datos["alm"]+" , "+datos["zona"]+","+datos["sector"]+", "+datos["est"]+""", 
            """+datos["pais"]+", "+str(sqlresult[0])+", '"+datos["desc"]+"""', 
            """+datos["tipo"]+ ", 'S', '"+fech_mod+"""', 
            '"""+ip_mod+ "', '"+datos["user"]+"');"
	
	cur.execute(insert)
	
	bdconn.commit()		

@route('/ListaTraslados/<mov>')
def ListaTraslados(mov):

	myReturnData = []
	
	cur.execute("""SELECT mov.cor_articulo_sec,art.cor_articulo_id 
				   FROM cor_inv_movimiento_detalle mov,cor_inv_articulo art
				   WHERE mov.cor_movimiento_sec="""+mov+";")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)
	
@route('/ListaInventario/<mov>')
def ListaInventario(mov):

	myReturnData = []
	
	cur.execute("""SELECT mat.cor_matcat_id,mov.cantidad 
				   FROM cor_inv_movimiento_detalle mov,cor_inv_matcat mat
				   WHERE mov.cor_movimiento_sec="""+mov+";")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"id":row[0],"cant":row[1]})
	return json.dumps(myReturnData)
	
@route('/ValidarInventario/<matcat>/<mov>')
def ValidarInventario(matcat,mov):

	myReturnData = []
	cur.execute("""SELECT cor_matcat_sec
				   FROM cor_inv_movimiento_detalle 
				   WHERE cor_movimiento_sec="""+mov+";")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			if(int(matcat)==row[0]):
				myReturnData.append({"error":'true'})
				return json.dumps(myReturnData)
	myReturnData.append({"error":'false'})
	return json.dumps(myReturnData)
	
@route('/ConsultaTipoResp')	
def ConsultaTipoResp():

	myReturnData = []
	
	cur.execute("SELECT cor_tiporesponsable_sec, cor_tiporesponsable_id FROM cor_crm_tipo_responsable; ")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)

@route('/CrearResp/<nombre>/<tipo>/<user>/<activo>')
def CrearResp(nombre,tipo,user,activo):

	myReturnData = []
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_crm_responsable_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	cur.execute(""" INSERT INTO cor_crm_responsable(
            cor_responsable_sec, cor_responsable_nombre, cor_tiporesponsable_sec, 
            cor_responsable_activo, cor_responsable_fec_mod, cor_responsable_ip_mod, 
            cor_responsable_login_mod)
    VALUES ("""+str(sqlresult[0])+ ", '"+nombre+"', "+tipo+""", 
            '"""+activo+"', '"+fech_mod+"', '"+ip_mod+"""', 
            '"""+user+"');")
	
	bdconn.commit()	
	
	myReturnData.append({"resp":sqlresult[0]})
	return json.dumps(myReturnData)
	
@route('/ConsultaMedioCom/<resp>')
def ConsultaMedioCom(resp):

	myReturnData = []
	
	cur.execute("""SELECT resp.cor_resp_mediocom_dato, me.cor_mediocom_id 
				   FROM cor_crm_responsable_mediocom resp, cor_crm_mediocom me
				   WHERE resp.cor_responsable_sec="""+resp+" and resp.cor_resp_mediocom_sec=me.cor_mediocomsec;")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"dato":row[0],"id":row[1]})
	return json.dumps(myReturnData)
		
@route('/CResponsable')
def CResponsable():
	myReturnData=[]
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("""SELECT res.cor_responsable_sec,res.cor_responsable_nombre,tipo.cor_tiporesponsable_id,tipo.cor_tiporesponsable_sec,res.cor_responsable_activo
				   FROM cor_crm_responsable res, cor_crm_tipo_responsable tipo
				   WHERE res.cor_tiporesponsable_sec=tipo.cor_tiporesponsable_sec;""")
	result = cur.fetchall()
	
	if (result==[]):
		print(json.dumps(myReturnData))
		#REV -> print $_GET['jsoncallback']. '('.json_encode($myReturnData).')';
	else:
		for row in result:
			myReturnData.append({"sec":row[0],"nombre":row[1],"tipoid":row[2],"tipo":row[3],"activo":row[4]})
	
	return json.dumps(myReturnData)

@route('/CMC')
def CMC():

	myReturnData = []
	
	cur.execute("SELECT cor_mediocomsec,cor_mediocom_id FROM cor_crm_mediocom WHERE cor_mediocom_activo='S';")
	result=cur.fetchall()
	if(result!=None):
		for row in result:
			myReturnData.append({"sec":row[0],"id":row[1]})
	return json.dumps(myReturnData)
	
@route('/AgregarMC/<resp>/<dato>/<tipo>/<user>')
def AgregarMC(resp,dato,tipo,user):

	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	sql = "SELECT nextval('cor_crm_responsable_mediocom_sec');"
	cur.execute(sql)
	sqlresult=cur.fetchone()
	
	cur.execute(""" INSERT INTO cor_crm_responsable_mediocom(
            cor_responsable_sec, cor_mediocom_sec, cor_resp_mediocom_sec, 
            cor_resp_mediocom_dato, cor_resp_mediocom_activo, cor_resp_mediocom_fec_mod, 
            cor_resp_mediocom_ip_mod, cor_resp_mediocom_login_mod)
    VALUES ("""+resp+","+tipo+", "+str(sqlresult[0])+""", 
            '"""+dato+"','S', '"+fech_mod+"""', 
            '"""+ip_mod+"','"+user+"');")
	
	bdconn.commit()	
	
@route('/ActResp/<tipo>/<nom>/<user>/<sec>')
def ActResp(tipo,nom,user,sec):
	
	fech_mod=str(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
	ip_mod=str(socket.gethostbyname(socket.gethostname()))
	
	cur.execute("""UPDATE cor_crm_responsable
				   SET cor_responsable_nombre='"""+nom+"', cor_tiporesponsable_sec="+tipo+""", 
					    cor_responsable_fec_mod='"""+fech_mod+"', cor_responsable_ip_mod='"+ip_mod+"""', 
					   cor_responsable_login_mod='"""+user+"""'
				   WHERE cor_responsable_sec="""+sec+";")
	bdconn.commit()	
		
	
# ************************************************* #
# **       WEB SERVERS - Alejandra Martini       ** #
# ************************************************* #

def calcRTprod(fec_limite, fec_prorroga):
	rt = 0
	fec_act = datetime.date.today()
	
	dr = (fec_limite - fec_act).days
	if (fec_prorroga != None):
		dr = (fec_prorroga - fec_act).days
		
	if (dr<0):	
		rt = abs((fec_limite - fec_act).days)
	
	return rt	
	
def calcRTproy(proy):
	rt = 0
	
	cur.execute("SELECT  cor_tareasec, cor_tarea_fec_entrega, cor_tarea_fec_prorroga FROM cor_proy_tarea WHERE cor_tarea_proyectosec = "+str(proy)+" AND cor_tarea_estatus_absoluto = 'A'")
	result = cur.fetchall()
		
	for row in result:
		rt = rt + calcRTprod(row[1], row[2])
	return rt

@route('/cargarlistaProy/<usr>')	
def cargarlistaProy(usr):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT priv_admin FROM cor_seg_users WHERE login ='"+usr+"';")
	result = cur.fetchone()

	if (result[0] == 'Y'):
		cur.execute("SELECT  cor_proyectosec FROM cor_proy_proyecto;")
		result = cur.fetchall()
	else:		
		cur.execute("SELECT  cor_proyectosec FROM cor_proy_proyecto p1 WHERE cor_proyecto_lider = '"+usr+"' OR '"+usr+"' IN (SELECT cor_os_responsable FROM cor_crm_os WHERE cor_os_tarea_proyectosec = p1.cor_proyectosec) OR '"+usr+"' IN (SELECT cor_proyecto_rrhh_login FROM cor_proy_proyecto_rrhh p0 WHERE p0.cor_proyectosec = p1.cor_proyectosec);")
		result = cur.fetchall()
		
	myReturnData.append({"proy":result})
	
	return json.dumps(myReturnData)
	
@route('/preReasignarTarea/<tarea>')
def preReasignarTarea(tarea):
	myReturnData=[];

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_os_id FROM cor_crm_os WHERE cor_ossec = "+tarea+";")
	result = cur.fetchone()
	
	myReturnData.append({"id":result[0]})
	
	cur.execute("SELECT login FROM cor_crm_os, cor_seg_users WHERE login=cor_os_responsable AND cor_ossec = "+tarea+";")
	resp = cur.fetchone()
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name FROM cor_seg_users WHERE login != '"+resp[0]+"' ORDER BY name;")
	result = cur.fetchall()
	
	for row in result:
		myReturnData.append({"usr":row[0]})
		
	return json.dumps(myReturnData)

@route('/reasignarTarea/<tarea>/<resp>/<obs>/<usr>')
def reasignarTarea(tarea,resp,obs,usr):	
	myReturnData=[];

	cur.execute("SELECT name FROM cor_crm_os, cor_seg_users WHERE login=cor_os_responsable AND cor_ossec = "+tarea+";")
	respV = cur.fetchone()
	# Query para actualizar el responsable de la tarea
	cur.execute("UPDATE cor_crm_os SET cor_os_responsable = (SELECT login FROM cor_seg_users WHERE name = '"+resp+"') WHERE cor_ossec = "+tarea+";")
		
	cur.execute("SELECT cor_os_empinstsec,cor_os_contactosec FROM cor_crm_os WHERE cor_ossec = "+tarea+";")
	emp_cont = cur.fetchone()
	observacion = "CAMBIO DE RESPONSABLE "+respV[0]+" A RESPONSABLE "+resp+": "+obs

	cur.execute("SELECT nextval('cor_crm_os_trk_sec')")
	sec_trk = cur.fetchone()
	
	cur.execute("INSERT into cor_crm_os_trk (cor_empinstsec,cor_contactosec,cor_ossec,cor_os_trksec,cor_os_trk_observacion,cor_os_trk_estatus_avancesec,cor_os_trk_fec_mod,cor_os_trk_login_mod,cor_os_trk_timesheet) values("+str(emp_cont[0])+","+str(emp_cont[1])+","+tarea+","+str(sec_trk[0])+",'"+observacion+"',1,'"+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+usr+"', 0);")
		
	bdconn.commit()	
	return json.dumps(myReturnData)	

@route('/listaProyAoC/<usr>/<estatus>')	
def listaProyAoC(usr,estatus):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT priv_admin FROM cor_seg_users WHERE login ='"+usr+"';")
	result = cur.fetchone()

	if (result[0] == 'Y'):
		cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_proyecto_estatus_absoluto FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto WHERE cor_proyecto_estatus_absoluto = '"+estatus+"'  ORDER BY  cor_proyecto_id;")
		result = cur.fetchall()
	else:		
		cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_proyecto_estatus_absoluto FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto p1 WHERE cor_proyecto_estatus_absoluto = '"+estatus+"' AND (cor_proyecto_lider = '"+usr+"' OR '"+usr+"' IN (SELECT cor_os_responsable FROM cor_crm_os WHERE cor_os_tarea_proyectosec = p1.cor_proyectosec) OR '"+usr+"' IN (SELECT cor_proyecto_rrhh_login FROM cor_proy_proyecto_rrhh p0 WHERE p0.cor_proyectosec = p1.cor_proyectosec)) ORDER BY  cor_proyecto_id;")
		result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"empinst":row[0],"id":row[1],"sec":row[2],"estatus_absoluto":row[3]})
	
	return json.dumps(myReturnData)
	
@route('/listaProyDetalle/<proy>')	
def listaProyDetalle(proy):
	myReturnData=[]

	cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyecto_desc, cor_proyecto_lider, cor_proyecto_estatus_absoluto FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto WHERE cor_proyectosec = '"+proy+"';")
	result = cur.fetchone()
	
	cur.execute("SELECT name FROM cor_seg_users WHERE login = '"+result[3]+"';")
	lider = cur.fetchone()

	if(result[4] == 'A'):
		estatus = "ABIERTO"
	else:
		estatus = "CERRADO"
		
	myReturnData.append({"empinst":result[0],"id":result[1],"descripcion":result[2],"lider":lider,"estatus_absoluto":estatus})
	
	return json.dumps(myReturnData)
	
@route('/listaProdAoC/<proy>/<estatus>')	
def listaProdAoC(proy,estatus):
	myReturnData=[]

	cur.execute("SELECT  cor_empinst_id, cor_tarea_id, cor_tareasec, cor_tarea_estatus_absoluto FROM cor_crm_empinst, cor_proy_tarea WHERE cor_tarea_empinstsec = cor_empinstsec AND cor_tarea_estatus_absoluto = '"+estatus+"' AND cor_tarea_proyectosec = '"+proy+"' ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"empinst":row[0],"id":row[1],"sec":row[2],"estatus_absoluto":row[3]})
	
	return json.dumps(myReturnData)
	
@route('/listaProdDetalle/<prod>')	
def listaProdDetalle(prod):
	myReturnData=[]

	cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_fase_id, cor_tarea_responsable, cor_tarea_id, cor_tarea_desc, cor_tarea_estatus_absoluto, cor_tarea_fp, cor_tarea_proy_horas_presupto FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto NATURAL JOIN cor_proy_fase, cor_proy_tarea  WHERE cor_tarea_fasesec = cor_fasesec AND cor_tarea_proyectosec = cor_proyectosec AND cor_tarea_empinstsec = cor_empinstsec AND cor_tareasec= '"+prod+"';")
	result = cur.fetchone()
	
	cur.execute("SELECT name FROM cor_seg_users WHERE login = '"+result[3]+"';")
	resp = cur.fetchone()

	if(result[6] == 'A'):
		estatus = "ABIERTO"
	else:
		estatus = "CERRADO"
	
	myReturnData.append({"empinst":result[0],"proy":result[1],"fase":result[2],"resp":resp,"id":result[4],"desc":result[5],"estatus":estatus,"fp":str(result[7]),"hp":str(result[8])})
	
	return json.dumps(myReturnData)
	
@route('/preReasignarProducto/<prod>')
def preReasignarProducto(prod):
	myReturnData=[];

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_tarea_id FROM cor_proy_tarea WHERE cor_tareasec = "+prod+";")
	result = cur.fetchone()
	
	myReturnData.append({"id":result[0]})
	
	cur.execute("SELECT login FROM cor_proy_tarea, cor_seg_users WHERE login = cor_tarea_responsable AND cor_tareasec = "+prod+";")
	resp = cur.fetchone()
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name FROM cor_seg_users WHERE login!= '"+resp[0]+"' ORDER BY name;")
	result = cur.fetchall()
	
	for row in result:
		myReturnData.append({"usr":row[0]})
		
	return json.dumps(myReturnData)

@route('/reasignarProducto/<prod>/<resp>/<obs>/<usr>')
def reasignarProducto(prod,resp,obs,usr):
	myReturnData=[];

	cur.execute("SELECT name FROM cor_proy_tarea, cor_seg_users WHERE login = cor_tarea_responsable AND cor_tareasec = "+prod+";")
	respV = cur.fetchone()
	# Query para actualizar el responsable de la tarea
	cur.execute("UPDATE cor_proy_tarea SET cor_tarea_responsable = (SELECT login FROM cor_seg_users WHERE name = '"+resp+"') WHERE cor_tareasec = "+prod+";")
		
	cur.execute("SELECT cor_tarea_empinstsec,cor_tarea_proyectosec,cor_tarea_fasesec FROM cor_proy_tarea WHERE cor_tareasec = "+prod+";")
	emp_proy_fase = cur.fetchone()
	observacion = "CAMBIO DE RESPONSABLE "+respV[0]+" A RESPONSABLE "+resp+": "+obs

	cur.execute("SELECT nextval('cor_proy_tarea_trk_sec')")
	sec_trk = cur.fetchone()
	
	cur.execute("INSERT into cor_proy_tarea_trk (cor_empinstsec,cor_proyectosec,cor_fasesec,cor_tareasec,cor_tarea_trksec,cor_tarea_trk_observacion,cor_tarea_trk_estatus_avancsec,cor_tarea_trk_fec_mod,cor_tarea_trk_login_mod) values("+str(emp_proy_fase[0])+","+str(emp_proy_fase[1])+","+str(emp_proy_fase[2])+","+prod+","+str(sec_trk[0])+",'"+observacion+"',1,'"+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+usr+"');")
		
	bdconn.commit()	
	return json.dumps(myReturnData)	
	
@route('/listaRecursosT/<tarea>')
def listaRecursosT(tarea):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name, cor_rrhh_rol_id FROM cor_crm_os_rrhh NATURAL JOIN cor_crm_rrhh_rol, cor_seg_users WHERE login = cor_os_rrhh_login AND cor_ossec ="+tarea+" ORDER BY name;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"rrhh":row[0],"rol":row[1]})
	
	return json.dumps(myReturnData)
	
@route('/selectTareaRRHH/<tarea>')
def selectTareaRRHH(tarea):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name FROM cor_seg_users WHERE login NOT IN (SELECT cor_os_rrhh_login FROM cor_crm_os_rrhh WHERE cor_ossec ="+tarea+") ORDER BY name;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"rrhh":row[0]})
	
	return json.dumps(myReturnData)
	
@route('/selectRoles')
def selectRoles():
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT cor_rrhh_rol_id FROM cor_crm_rrhh_rol ORDER BY cor_rrhh_rol_id;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"rol":row[0]})
	
	return json.dumps(myReturnData)
	
@route('/insertarRRHHtarea/<tarea>/<usr>/<rrhh>/<rol>')
def insertarRRHHtarea(tarea,usr,rrhh,rol):
	myReturnData=[];
  
	cur.execute("SELECT cor_os_empinstsec, cor_os_contactosec FROM cor_crm_os WHERE cor_ossec = "+tarea+";")
	emp_cont = cur.fetchone()

	cur.execute("SELECT login FROM cor_seg_users WHERE name = '"+rrhh+"';")
	rrhh_login = cur.fetchone()
	
	cur.execute("SELECT cor_rrhh_rolsec FROM cor_crm_rrhh_rol WHERE cor_rrhh_rol_id = '"+rol+"';")
	rrhh_rol = cur.fetchone()
	
	cur.execute("SELECT nextval('cor_crm_os_rrhh_sec')")
	sec_rrhh = cur.fetchone()
	
	cur.execute("INSERT INTO cor_crm_os_rrhh (cor_empinstsec,cor_contactosec,cor_ossec,cor_os_rrhh_login,cor_rrhh_rolsec,cor_os_rrhh_sms,cor_os_rrhh_email,cor_os_rrhh_fec_mod,cor_os_rrhh_login_mod,cor_os_rrhhsec) values("+str(emp_cont[0])+","+str(emp_cont[0])+","+tarea+",'"+str(rrhh_login[0])+"',"+str(rrhh_rol[0])+",'S','S','"+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+usr+"',"+str(sec_rrhh[0])+");")
		
	bdconn.commit()	
	return json.dumps(myReturnData)

@route('/listaRecursosP/<proy>')
def listaRecursosP(proy):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name, cor_rrhh_rol_id FROM cor_proy_proyecto_rrhh NATURAL JOIN cor_crm_rrhh_rol, cor_seg_users WHERE login = cor_proyecto_rrhh_login AND cor_proyectosec ="+proy+" ORDER BY name;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"rrhh":row[0],"rol":row[1]})
	
	return json.dumps(myReturnData)
	
@route('/selectProyRRHH/<proy>')
def selectProyRRHH(proy):
	myReturnData=[]

	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT name FROM cor_seg_users WHERE login NOT IN (SELECT cor_proyecto_rrhh_login FROM cor_proy_proyecto_rrhh WHERE cor_proyectosec ="+proy+") ORDER BY name;")
	result = cur.fetchall()
		
	for row in result:
		myReturnData.append({"rrhh":row[0]})
	
	return json.dumps(myReturnData)
	
@route('/insertarRRHHproy/<proy>/<usr>/<rrhh>/<rol>')
def insertarRRHHproy(proy,usr,rrhh,rol):
	myReturnData=[];
  
	cur.execute("SELECT cor_empinstsec FROM cor_proy_proyecto WHERE cor_proyectosec = "+proy+";")
	emp = cur.fetchone()

	cur.execute("SELECT login FROM cor_seg_users WHERE name = '"+rrhh+"';")
	rrhh_login = cur.fetchone()
	
	cur.execute("SELECT cor_rrhh_rolsec FROM cor_crm_rrhh_rol WHERE cor_rrhh_rol_id = '"+rol+"';")
	rrhh_rol = cur.fetchone()
	
	cur.execute("SELECT nextval('cor_proy_proyecto_rrhh_sec')")
	sec_rrhh = cur.fetchone()
	
	cur.execute("INSERT INTO cor_proy_proyecto_rrhh (cor_empinstsec,cor_proyectosec,cor_proyecto_rrhh_login,cor_rrhh_rolsec,cor_proyecto_rrhh_fec_mod,cor_proyecto_rrhh_login_mod,cor_proyecto_rrhh_sms,cor_proyecto_rrhh_email,cor_proyecto_rrhhsec) values("+str(emp[0])+","+proy+",'"+str(rrhh_login[0])+"',"+str(rrhh_rol[0])+",'"+datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")+"','"+usr+"','S','S',"+str(sec_rrhh[0])+");")
		
	bdconn.commit()	
	return json.dumps(myReturnData)
	
@route('/listaEstProy/<usr>')	
def listaEstProy(usr):
	myReturnData=[]
	nro=0;
	# proyectos de los q el usr es lider
	cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_empinstsec FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto WHERE cor_proyectosec != 1 AND cor_proyecto_estatus_absoluto = 'A' AND cor_proyecto_lider = '"+usr+"' ORDER BY  cor_proyecto_id;")
	result = cur.fetchall()

	nro = nro + len(result)
	myReturnData.append({"tam":3,"lista":1,"op":"Proyectos Liderizados","cant":len(result)})
	for row in result:
		logro = estadisticas.fg_estadisticas_0003_logro_proyecto(row[3], row[2])
		gasto = estadisticas.fg_estadisticas_0006_gasto_proyecto(row[3], row[2])
		rt = calcRTproy(row[2])
		gastoInt = 0.0
		myReturnData.append({"tam":7,"cliente":row[0],"id":row[1],"sec":row[2],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt})
		
	# proyectos de los q el usr es RRHH
	cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_empinstsec FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto p1 WHERE cor_proyectosec != 1 AND cor_proyecto_lider != '"+usr+"' AND cor_proyecto_estatus_absoluto = 'A' AND  '"+usr+"' IN (SELECT cor_proyecto_rrhh_login FROM cor_proy_proyecto_rrhh p0 WHERE p0.cor_proyectosec = p1.cor_proyectosec) ORDER BY  cor_proyecto_id;")
	result = cur.fetchall()
	nro = nro + len(result)
	
	cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_empinstsec FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto p1 WHERE cor_proyectosec != 1 AND cor_proyecto_estatus_absoluto = 'A' AND  '"+usr+"' IN (SELECT cor_proyecto_rrhh_login FROM cor_proy_proyecto_rrhh p0 WHERE p0.cor_proyectosec = p1.cor_proyectosec) ORDER BY  cor_proyecto_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":2,"op":"RRHH Proyecto","cant":len(result)})
	for row in result:
		rt = calcRTproy(row[2])
		logro = estadisticas.fg_estadisticas_0003_logro_proyecto(row[3], row[2])
		gasto = estadisticas.fg_estadisticas_0006_gasto_proyecto(row[3], row[2])
		gastoInt = 0.0
		myReturnData.append({"tam":7,"cliente":row[0],"id":row[1],"sec":row[2],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt})
		
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT priv_admin FROM cor_seg_users WHERE login ='"+usr+"';")
	result = cur.fetchone()

	# otros proyectos
	if (result[0] == 'Y'):
		cur.execute("SELECT  cor_empinst_id, cor_proyecto_id, cor_proyectosec, cor_empinstsec FROM cor_crm_empinst NATURAL JOIN cor_proy_proyecto p1 WHERE cor_proyectosec != 1 AND cor_proyecto_estatus_absoluto = 'A' AND cor_proyectosec NOT IN (SELECT cor_proyectosec FROM cor_proy_proyecto_rrhh  WHERE cor_proyecto_rrhh_login = '"+usr+"') ORDER BY  cor_proyecto_id;")
		result = cur.fetchall()	
		
		myReturnData.append({"tam":3,"lista":3,"op":"Otros Proyectos","cant":len(result)})
		for row in result:
			rt = calcRTproy(row[2])
			logro = estadisticas.fg_estadisticas_0003_logro_proyecto(row[3], row[2])
			gasto = estadisticas.fg_estadisticas_0006_gasto_proyecto(row[3], row[2])
			gastoInt = 0.0
			myReturnData.append({"tam":7,"cliente":row[0],"id":row[1],"sec":row[2],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt})
	
	myReturnData.append({"tam":1, "cant":nro})	
	return json.dumps(myReturnData)
	
@route('/listaEstProd/<usr>')	
def listaEstProd(usr):
	myReturnData=[]
	
	nro=0;
	# productos de los q el usr es responsable
	cur.execute("SELECT  cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea, cor_seg_users WHERE login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_estatus_absoluto = 'A' AND cor_tarea_responsable = '"+usr+"' ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
	
	nro=len(result)
	myReturnData.append({"tam":3,"lista":1,"op":"Responsable Producto","cant":len(result)})
	for row in result:
		logro = estadisticas.fg_estadisticas_0001_logro_producto(row[0], row[1], row[2], row[3])
		gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[0], row[1], row[2], row[3])
		rt = calcRTprod(row[8],row[7])
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[4],2)),"id":row[5],"sec":row[3],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[6]})
		
	# productos de los q el usr es RRHH
	cur.execute("SELECT  cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea NATURAL JOIN cor_proy_tarea_rrhh, cor_seg_users WHERE login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_responsable != '"+usr+"' AND cor_tarea_estatus_absoluto = 'A' AND  '"+usr+"' = cor_tarea_rrhh_login ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
	nro = nro + len(result)	
	
	cur.execute("SELECT  cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea NATURAL JOIN cor_proy_tarea_rrhh, cor_seg_users WHERE login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_estatus_absoluto = 'A' AND  '"+usr+"' = cor_tarea_rrhh_login ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
	myReturnData.append({"tam":3,"lista":2,"op":"RRHH Producto","cant":len(result)})
	for row in result:
		logro = estadisticas.fg_estadisticas_0001_logro_producto(row[0], row[1], row[2], row[3])
		gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[0], row[1], row[2], row[3])
		rt = calcRTprod(row[8],row[7])
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[4],2)),"id":row[5],"sec":row[3],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[6]})
	
	# productos asociados a proyectos donde el usr es lider
	
	cur.execute("SELECT  cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea, cor_seg_users, cor_proy_proyecto WHERE cor_proyectosec != 1 AND cor_tarea_proyectosec = cor_proyectosec AND cor_proyecto_lider = '"+usr+"' AND login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_estatus_absoluto = 'A' ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
			
	myReturnData.append({"tam":3,"lista":2,"op":"Proyectos Liderizados","cant":len(result)})
	for row in result:
		logro = estadisticas.fg_estadisticas_0001_logro_producto(row[0], row[1], row[2], row[3])
		gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[0], row[1], row[2], row[3])
		rt = calcRTprod(row[8],row[7])
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[4],2)),"id":row[5],"sec":row[3],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[6]})
	
	# productos asociados a proyectos donde el usr es RRHH
	
	cur.execute("SELECT  cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea, cor_seg_users, cor_proy_proyecto WHERE cor_proyectosec != 1 AND cor_tarea_proyectosec = cor_proyectosec AND login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_estatus_absoluto = 'A' AND cor_proyectosec IN (SELECT cor_proyectosec FROM cor_proy_proyecto_rrhh WHERE cor_proyecto_rrhh_login = '"+usr+"') ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
		
	myReturnData.append({"tam":3,"lista":2,"op":"RRHH Proyecto","cant":len(result)})
	for row in result:
		logro = estadisticas.fg_estadisticas_0001_logro_producto(row[0], row[1], row[2], row[3])
		gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[0], row[1], row[2], row[3])
		rt = calcRTprod(row[8],row[7])
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[4],2)),"id":row[5],"sec":row[3],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[6]})
	
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT priv_admin FROM cor_seg_users WHERE login ='"+usr+"';")
	result = cur.fetchone()

	# otros productos
	if (result[0] == 'Y'):
	
		cur.execute("SELECT cor_tarea_empinstsec, cor_tarea_proyectosec, cor_tarea_fasesec, cor_tareasec, cor_tarea_fp, cor_tarea_id, name, cor_tarea_fec_prorroga, cor_tarea_fec_entrega FROM cor_proy_tarea, cor_seg_users, cor_proy_proyecto y1 WHERE cor_proyectosec != 1 AND cor_tarea_proyectosec = cor_proyectosec AND cor_proyecto_lider != '"+usr+"' AND login = cor_tarea_responsable AND cor_tareasec != 1 AND cor_tarea_responsable != '"+usr+"' AND cor_tarea_estatus_absoluto = 'A' AND cor_tareasec NOT IN (SELECT cor_tareasec FROM cor_proy_tarea_rrhh WHERE cor_tarea_rrhh_login = '"+usr+"') AND cor_proyectosec NOT IN (SELECT cor_proyectosec FROM cor_proy_proyecto_rrhh WHERE cor_proyecto_rrhh_login = '"+usr+"') ORDER BY  cor_tarea_id;")
		result = cur.fetchall()	
		
		myReturnData.append({"tam":3,"lista":5,"op":"Otros Productos","cant":len(result)})
		for row in result:
			logro = estadisticas.fg_estadisticas_0001_logro_producto(row[0], row[1], row[2], row[3])
			gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[0], row[1], row[2], row[3])
			rt = calcRTprod(row[8],row[7])
			gastoInt = 0.0
			myReturnData.append({"tam":8,"fp":str(round(row[4],2)),"id":row[5],"sec":row[3],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[6]})
	
	myReturnData.append({"tam":1, "cant":nro})	
	return json.dumps(myReturnData)

@route('/listaEstTarea/<usr>')	
def listaEstTarea(usr):
	myReturnData=[]
	nro = 0;
	# tareas en espera de algun RRHH
	
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_holder_login = '"+usr+"' ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":0,"op":"En Espera","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	
	# tareas de las que el usr es responsable (Asociadas a un proy)
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_responsable = '"+usr+"' AND cor_os_empinstsec = cor_os_tarea_empinstsec ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	nro= nro + len(result)
	myReturnData.append({"tam":3,"lista":1,"op":"Responsable Tarea","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# tareas de las que el usr es RRHH (Asociadas a un proy)
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_responsable != '"+usr+"' AND cor_os_empinstsec = cor_os_tarea_empinstsec AND cor_ossec IN (SELECT cor_ossec FROM cor_crm_os_rrhh WHERE cor_os_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result = cur.fetchall()	
	nro= nro + len(result)
	
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_empinstsec = cor_os_tarea_empinstsec AND cor_ossec IN (SELECT cor_ossec FROM cor_crm_os_rrhh WHERE cor_os_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":2,"op":"RRHH Tarea","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# tareas asociadas a productos de los que el usr es responsable
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance, cor_proy_tarea WHERE cor_tareasec != 1 AND login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_tarea_tareasec = cor_tareasec AND cor_tarea_responsable = '"+usr+"' ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":3,"op":"Responsable Producto","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# tareas asociadas a productos de los q el usr es RRHH
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance, cor_proy_tarea WHERE cor_tareasec != 1 AND login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_tarea_tareasec = cor_tareasec AND cor_tareasec IN (SELECT cor_tareasec FROM cor_proy_tarea_rrhh WHERE cor_tarea_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":3,"op":"RRHH Producto","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# tareas asociadas a proyectos donde el usr es lider
	
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance, cor_proy_proyecto WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_tarea_proyectosec = cor_proyectosec AND cor_proyectosec != 1 AND cor_proyecto_lider = '"+usr+"' ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":3,"op":"Lider Proyecto","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# tareas asociadas a proyectos donde el usr es RRHH
	
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance, cor_proy_proyecto WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_tarea_proyectosec = cor_proyectosec AND cor_proyectosec != 1 AND cor_os_tarea_proyectosec IN (SELECT cor_proyectosec FROM cor_proy_proyecto_rrhh WHERE cor_proyecto_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	myReturnData.append({"tam":3,"lista":3,"op":"RRHH Proyecto","cant":len(result)})
	for row in result:
		logro = 0.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	# TAREAS sin asociar a un proy
	
	# usr responsable
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_responsable = '"+usr+"' AND cor_os_empinstsec != cor_os_tarea_empinstsec ORDER BY  cor_os_id;")
	result = cur.fetchall()
	nro= nro + len(result)
	
	# usr RRHH
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_responsable != '"+usr+"' AND cor_os_empinstsec != cor_os_tarea_empinstsec AND cor_ossec IN (SELECT cor_ossec FROM cor_crm_os_rrhh WHERE cor_os_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result1 = cur.fetchall()
	nro= nro + len(result1)
	
	cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_empinstsec != cor_os_tarea_empinstsec AND cor_ossec IN (SELECT cor_ossec FROM cor_crm_os_rrhh WHERE cor_os_rrhh_login = '"+usr+"') ORDER BY  cor_os_id;")
	result1 = cur.fetchall()
	
	nro= nro + len(result) + len(result1)
	myReturnData.append({"tam":3,"lista":1.2,"op":"Sin proyectos asociados","cant":(len(result) + len(result1))})
	for row in result:
		logro = 100.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	for row in result1:
		logro = 100.0
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
		fec_act = datetime.date.today()
		rt = (row[9].date() - fec_act).days
		gastoInt = 0.0
		myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	
	# Consulta de la BD para obtener datos como objetos de Python
	cur.execute("SELECT priv_admin FROM cor_seg_users WHERE login ='"+usr+"';")
	result = cur.fetchone()

	# otras tareas
	if (result[0] == 'Y'):
	
		cur.execute("SELECT  cor_os_empinstsec, cor_os_tarea_empinstsec, cor_os_tarea_proyectosec, cor_os_tarea_fasesec, cor_os_tarea_tareasec, cor_ossec, cor_os_fp, cor_os_id, name, cor_os_fecha_limite FROM  cor_crm_os, cor_seg_users, cor_crm_estatus_avance, cor_proy_tarea, cor_proy_proyecto WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_estatus_absoluto = 'A' AND cor_os_responsable != '"+usr+"' AND cor_ossec NOT IN (SELECT cor_ossec FROM cor_crm_os_rrhh WHERE cor_os_rrhh_login = '"+usr+"') AND cor_os_tarea_tareasec = cor_tareasec AND cor_tarea_responsable != '"+usr+"' AND cor_tareasec NOT IN (SELECT cor_tareasec FROM cor_proy_tarea_rrhh WHERE cor_tarea_rrhh_login = '"+usr+"' AND cor_tareasec !=1) AND cor_os_tarea_proyectosec = cor_proyectosec AND cor_proyecto_lider != '"+usr+"' AND cor_os_tarea_proyectosec NOT IN (SELECT cor_proyectosec FROM cor_proy_proyecto_rrhh WHERE cor_proyecto_rrhh_login = '"+usr+"' AND cor_proyectosec !=1) ORDER BY  cor_os_id;")
		result = cur.fetchall()	
		
		myReturnData.append({"tam":3,"lista":5,"op":"Otras Tareas","cant":len(result)})
		for row in result:
			logro = 0.0
			gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[5])
			fec_act = datetime.date.today()
			rt = (row[9].date() - fec_act).days
			gastoInt = 0.0
			myReturnData.append({"tam":8,"fp":str(round(row[6],2)),"id":row[7],"sec":row[5],"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoInt":str(round(gastoInt,2)),"rt":rt, "resp":row[8]})
	
	myReturnData.append({"tam":1, "cant":nro})	
	return json.dumps(myReturnData)

@route('/listaFases_Est/<proy>')	
def listaFases_Est(proy):
	myReturnData=[]

	cur.execute("SELECT  cor_fase_id, cor_fasesec, cor_fase_fp, cor_empinstsec FROM cor_proy_fase WHERE cor_proyectosec='"+proy+"'ORDER BY  cor_fase_id;")
	result = cur.fetchall()
		
	for row in result:
		logro = estadisticas.fg_estadisticas_0002_logro_fase(row[3],proy,row[1])
		gasto = estadisticas.fg_estadisticas_0005_gasto_fase(row[3],proy,row[1])
		gastoI = 0.0
		rt = 0
		myReturnData.append({"lista":1,"id":row[0],"sec":row[1],"fp":str(round(row[2])),"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoI":str(round(gastoI,2)),"rt":rt})
	
	cur.execute("SELECT  cor_empinstsec FROM cor_proy_proyecto WHERE cor_proyectosec='"+proy+"'")
	result = cur.fetchone()
	
	tarT=0
	tarC=0
	
	cur.execute("SELECT COUNT(*) FROM cor_crm_os WHERE cor_os_tarea_proyectosec='"+proy+"'")
	tar_T = cur.fetchone()
	
	cur.execute("SELECT COUNT(*) FROM cor_crm_os, cor_crm_estatus_avance WHERE cor_os_estatus_avancesec = cor_estatus_avancesec	AND cor_estatus_absoluto = 'C' AND cor_os_tarea_proyectosec='"+proy+"'")
	tar_C = cur.fetchone()
	
	if(tar_T != None):
		tarT=tar_T[0]
	if(tar_C != None):
		tarC=tar_C[0]
	
	gasto = estadisticas.fg_estadisticas_0006_gasto_proyecto(result[0], proy)
	gastoInt = 0.0
	logro = estadisticas.fg_estadisticas_0003_logro_proyecto(result[0], proy)
	rt = rt = calcRTproy(proy)
	hp = estadisticas.fg_proy_0001_calcular_hp_proy(result[0], proy)
	hpex = estadisticas.fg_proy_0003_calcular_hex_proy(result[0], proy, hp)
	hc = estadisticas.fg_proy_0002_calcular_hc_proy(result[0], proy)
	rendimiento = 20
	fecha_ini = estadisticas.fg_proy_0014_fechaINI_proy(result[0], proy)
	fecha_fin = estadisticas.fg_proy_0015_fechaFIN_proy(result[0], proy)
	bt = int(hp - hc)
	
	myReturnData.append({"lista":2,"gastoInt":str(round(gastoInt,2)),"gasto":str(round(gasto,2)), "logro":str(round(logro,2)) ,"rt":int(rt),"hp":int(hp),"hpex":int(hpex),"hc":int(hc), "rendimiento":rendimiento,"fecha_ini":str(fecha_ini),"fecha_fin":str(fecha_fin.date()),"tarT":tarT,"tarC":tarC,"bt":bt})
	return json.dumps(myReturnData)

@route('/listaProd_Est/<proy>/<fase>')	
def listaProd_Est(proy,fase):
	myReturnData=[]

	cur.execute("SELECT  cor_tarea_id, cor_tareasec, cor_tarea_fp, cor_tarea_empinstsec, name FROM cor_proy_tarea, cor_seg_users WHERE cor_tarea_responsable=login AND cor_tarea_proyectosec='"+proy+"' AND cor_tarea_fasesec='"+fase+"' ORDER BY  cor_tarea_id;")
	result = cur.fetchall()
		
	for row in result:
		logro = estadisticas.fg_estadisticas_0001_logro_producto(row[3],proy,fase,row[1])
		gasto = estadisticas.fg_estadisticas_0004_gasto_producto(row[3],proy,fase,row[1])
		gastoI = 0.0
		rt = 0
		myReturnData.append({"id":row[0],"sec":row[1],"fp":str(round(row[2])),"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoI":str(round(gastoI,2)),"rt":rt, "resp":row[4]})
	
	return json.dumps(myReturnData)
	
@route('/listaTarea_Est/<prod>')	
def listaTarea_Est(prod):
	myReturnData=[]

	cur.execute("SELECT  cor_os_id, cor_ossec, cor_os_fp, cor_estatus_absoluto, name FROM cor_crm_os, cor_seg_users, cor_crm_estatus_avance WHERE login = cor_os_responsable AND cor_os_estatus_avancesec = cor_estatus_avancesec AND cor_os_tarea_tareasec='"+prod+"' ORDER BY  cor_os_id;")
	result = cur.fetchall()
	
	cur.execute("SELECT cor_proyectosec, cor_proyecto_id, cor_fase_id, cor_fasesec, cor_empinstsec, cor_tarea_proy_horas_presupto, cor_tarea_fec_entrega, cor_tarea_fec_prorroga FROM cor_proy_proyecto NATURAL JOIN cor_proy_fase, cor_proy_tarea WHERE cor_tareasec = '"+prod+"' AND cor_tarea_proyectosec = cor_proyectosec AND cor_tarea_fasesec = cor_fasesec")
	dat = cur.fetchone()
	
	for row in result:
		if(row[3] == 'A'):
			logro = 0.0
		else:
			logro = 100.0
			
		gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(row[1])
		gastoI = 0.0
		rt = 0
		myReturnData.append({"lista":1,"id":row[0],"sec":row[1],"fp":str(round(row[2])),"logro":str(round(logro,2)),"gasto":str(round(gasto,2)),"gastoI":str(round(gastoI,2)),"rt":rt, "resp":row[4]})
	
	myReturnData.append({"lista":1,"sec":"title","proy":dat[0],"proy_id":dat[1],"fase_id":dat[2]})
	
	tarT=0
	tarC=0
	
	cur.execute("SELECT COUNT(*) FROM cor_crm_os WHERE cor_os_tarea_tareasec='"+prod+"'")
	tar_T = cur.fetchone()
	
	cur.execute("SELECT COUNT(*) FROM cor_crm_os, cor_crm_estatus_avance WHERE cor_os_estatus_avancesec = cor_estatus_avancesec	AND cor_estatus_absoluto = 'C' AND cor_os_tarea_tareasec='"+prod+"'")
	tar_C = cur.fetchone()
	
	if(tar_T != None):
		tarT=tar_T[0]
	if(tar_C != None):
		tarC=tar_C[0]
	
	gasto = estadisticas.fg_estadisticas_0004_gasto_producto(dat[4], dat[0], dat[3], prod)
	logro = estadisticas.fg_estadisticas_0001_logro_producto(dat[4], dat[0], dat[3], prod)
	rt = calcRTprod(dat[6],dat[7])
	hp = dat[5]
	hpex = estadisticas.fg_proy_0010_calcular_hex_tarea(dat[4], dat[0], dat[3], prod, hp)
	hc = estadisticas.fg_proy_0009_calcular_hc_tarea(dat[4], dat[0], dat[3], prod)
	rendimiento = 20
	gastoInt = 0.0
	fecha_ini = estadisticas.fg_proy_0018_fechaINI_prod(dat[4], dat[0], dat[3], prod)
	fecha_fin = estadisticas.fg_proy_0019_fechaFIN_prod(dat[4], dat[0], dat[3], prod)
	bt = int(hp - hc)
	
	myReturnData.append({"lista":2,"gastoInt":str(round(gastoInt,2)),"gasto":str(round(gasto,2)), "logro":str(round(logro,2)) ,"rt":int(rt),"hp":int(hp),"hpex":int(hpex),"hc":int(hc), "rendimiento":rendimiento,"fecha_ini":str(fecha_ini),"fecha_fin":str(fecha_fin.date()),"tarT":tarT,"tarC":tarC,"bt":bt})
	return json.dumps(myReturnData)
	
@route('/datosTarea_Est/<tarea>')
def datosTarea_Est(tarea):
	myReturnData=[]
	
	cur.execute("SELECT cor_proyectosec, cor_proyecto_id, cor_fase_id, cor_tareasec, cor_tarea_id, cor_os_fecha_limite, cor_os_horas_planificadas, cor_os_fec_inicio FROM cor_proy_proyecto NATURAL JOIN cor_proy_fase, cor_proy_tarea, cor_crm_os WHERE cor_ossec = '"+tarea+"' AND cor_proyectosec = cor_os_tarea_proyectosec AND cor_fasesec = cor_os_tarea_fasesec AND cor_tareasec = cor_os_tarea_tareasec")
	dat = cur.fetchone()
	
	gasto = estadisticas.fg_estadisticas_0007_gasto_tarea(tarea)
	gastoInt = 0.0
	dr = (dat[5].date() - datetime.date.today()).days
	hp = dat[6]	
	hc = estadisticas.fg_proy_0012_calcular_hc_os(tarea)
	
	hpex = hc - hp
	if (hpex < 0):
		hpex = 0
	
	rendimiento = 50
	bt = int(hp - hc)
	
	myReturnData.append({"sec":"title","proy":dat[0],"proy_id":dat[1],"fase_id":dat[2],"prod":dat[3],"prod_id":dat[4],"gasto":str(round(gasto,2)),"dr":int(dr),"hp":int(hp),"hpex":int(hpex),"hc":int(hc), "rendimiento":rendimiento,"gastoInt":str(round(gastoInt,2)),"fecha_ini":str(dat[7]),"fecha_fin":str(dat[5].date()),"bt":bt})
	
	return json.dumps(myReturnData)
	
# ************************************************* #
# **      Programa Principal del WEB SERVER      ** #
# ************************************************* #

# Abriendo conexion con la BD
try:
	bdconn = psycopg2.connect("dbname=cor_processu_ user=postgres password=root host=sid_db_02")
except:
	response.write("error en la conexion con la BD")
cur = bdconn.cursor()

run(host=socket.gethostname(), port=8001)
#run(host='localhost', port=8000)
