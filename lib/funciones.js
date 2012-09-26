/* PROYECTO: COR PROCESSU MOBILE */
var IP = "corprocessu.com"
var puerto = "8002"
//var IP = "10.20.99.68"
//var IP = "192.168.1.101"
//var puerto = "8000"
var clave = "sid1029";

/*********************************************************************************************************************************/
/*****                                                 FUNCIONES - Álvaro Parada                                             *****/
/*****                                                 		MÓDULO INVENTARIO  												 *****/
/*********************************************************************************************************************************/
function maximaLongitud(texto,maxlong) {
	var tecla, in_value, out_value;
	if (texto.value.length > maxlong) {
		in_value = texto.value;
		out_value = in_value.substring(0,maxlong);
		texto.value = out_value;
		return false;
	}
	return true;
}

function CAlmacen(){
	//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0001_consulta_almacen",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="ConsultaA('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.resp+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Almacenes: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Almacen');
		redireccionar("CAlmacen.html\#CAlmacen");
	});	
}


function ConsultaA(sec){
	localStorage.almacen=sec;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0005_consulta_detalle_almacen",{"TK":clave,"sec":sec},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		localStorage.id=miRes[0].id;
		localStorage.desc=miRes[0].desc;
		localStorage.tipo=miRes[0].tipo;
		localStorage.resp=miRes[0].resp;
		localStorage.activo=miRes[0].activo;
		localStorage.ref="CAlmacen.html";
		redireccionar("Almacen.html");
	});
}

function almacen(arg,ref){
	localStorage.ref=ref;
	localStorage.id="";
	localStorage.desc="";
	localStorage.tipo="F";
	localStorage.activo="S";
	localStorage.resp="";
	localStorage.almacen="";
	redireccionar(arg);
}

//New 17/09/2012 Alvaro
function ActualizarAlm(){

	if(document.getElementById("radio1_0").checked){
		localStorage.tipo="F";
	}else if(document.getElementById("radio1_1").checked){
		localStorage.tipo="V";
	}
	select=document.getElementById("res").options;
	index=document.getElementById("res").selectedIndex;
	localStorage.resp=select[index].value;

	if(document.getElementById("activo").checked){
		localStorage.activo="S";
	}else{
		localStorage.activo="N";
	}
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0003_modifica_almacen",{"TK":clave,"tipo":localStorage.tipo,"activo":localStorage.activo,"resp_sec":localStorage.resp,"user":localStorage.nombre,"sec":localStorage.almacen},function(resultado){
		redireccionar("Almacen.html");
	});
}

function PreNewAlmacen(){

	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0001_consulta_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		readonly = "";
		if(localStorage.almacen!=""){
			readonly="readonly";
		}
		html='<label for="id">Identificador:</label><input type="text" name="id" id="id" value="'+localStorage.id+'" '+readonly+' maxlength="32"/>';
		html+='<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" '+readonly+' onKeyUp="return maximaLongitud(this,100)">'+localStorage.desc+'</textarea>';
		if(localStorage.tipo=="F"){
			html+='<div data-role="fieldcontain"><fieldset data-role="controlgroup"><legend>Tipo:</legend><input type="radio" name="radio1" id="radio1_0" value="F" checked/><label for="radio1_0" che>Físico</label><input type="radio" name="radio1" id="radio1_1" value="V" /><label for="radio1_1">Virtual</label></fieldset></div>';
		}else if (localStorage.tipo=="V"){
			html+='<div data-role="fieldcontain"><fieldset data-role="controlgroup"><legend>Tipo</legend><input type="radio" name="radio1" id="radio1_0" value="F" /><label for="radio1_0" che>Fisico</label><input type="radio" name="radio1" id="radio1_1" value="V" checked/><label for="radio1_1">Virtual</label></fieldset></div>';
		}
		html+='<label for="res">Responsable:</label><select name="res" id="res" data-theme="b"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			if(localStorage.resp==elemento.sec){
				html+='<option value="'+elemento.sec+'" selected>'+elemento.id+'</option>';
			}else{
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			}
		});
		if(localStorage.activo=="S"){
			html+='</select> <label for="activo">Activo </label> <input type="checkbox" name="activo" id="activo" value="S" checked/>';
		}else{
			html+='</select> <label for="activo">Activo </label> <input type="checkbox" name="activo" id="activo" value="S"/>';
		}
		if(localStorage.almacen!="") {
			html+='<button onClick="ActualizarAlm()" data-theme = "b" align="center">Actualizar</button>';
			html+='<div data-role="collapsible-set"><div data-role="collapsible" data-collapsed="true" data-theme="b"><h3>Ubicación</h3><p><button onClick="redireccionar(\'NewZonaAlmacen.html\')" data-theme="b"> Agregar Ubicación </button>';

			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_zona_0001_consulta_ubicacion",{"TK":clave,"almacen":localStorage.almacen,"tipo":"almacen"},function(resultado1){
				miRes1 = jQuery.parseJSON(resultado1);
				html+='<div data-role="content" id="lista" aling="center"><ul data-role="listview" data-inset="true" id="Listazona">';
				$.each(miRes1, function(i, elemento){
					html+='<li><a onClick="ConsultaU('+elemento.sec+')"><h3>'+elemento.z1+', '+elemento.z2+', '+elemento.z3+', '+elemento.z4+'</h3><p>Tipo: '+elemento.tipo+'</p></a></li>';
				});
				html+='</ul></div></p></div></div>';			
				$(html).appendTo('#Alm');
				redireccionar('Almacen.html\#Almacen');
			});
		}else{
			html+='<button onClick="NewAlmacen()" data-theme="b"> Crear Almacén </button>';
			$(html).appendTo('#Alm');
			redireccionar('Almacen.html\#Almacen');
			
		}	
	});
}

function ConsultaU(sec) {
	localStorage.w = sec;
	redireccionar('consultaU.html');
}

function ConsultarU () {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_zona_0002_consulta_ubicacion_detalle",{"TK":clave,"almacen":localStorage.almacen,"tipo":"almacen","sec":localStorage.w},function(resultado1){
		miRes1 = jQuery.parseJSON(resultado1); html = '';
		$.each(miRes1, function(i, elemento){
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv1_0001_consulta_zona_n1",{"TK":clave},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				html += '<label for="pais" class="select">País:</label><select name="pais" id="pais" onChange="NewZonaAlmacenz2()" data-theme="b"><option value="">Seleccione</option>';
				$.each(miRes2, function(i, elemento2){
					if (elemento.z1s == elemento2.sec) {
						html += '<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
					} else {
						html += '<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
					}
				});
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n2",{"TK":clave,"pais":elemento.z1s},function(resultado3){
					html+='</select>';
					miRes3 = jQuery.parseJSON(resultado3);
					html += '<label for="est" class="select">Estado/Ciudad:</label><select name="est" id="est" onChange="NewZonaAlmacenz3()" data-theme="b"><option value="">Seleccione</option>';
					$.each(miRes3, function(i, elemento3){
						if (elemento.z2s == elemento3.sec) {
							html += '<option value="'+elemento3.sec+'" selected>'+elemento3.id+'</option>';
						} else {
							html += '<option value="'+elemento3.sec+'">'+elemento3.id+'</option>';
						}
					});
				   jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n3",{"TK":clave,"pais":elemento.z1s,"est":elemento.z2s},function(resultado4){
						html+='</select>';
						miRes4 = jQuery.parseJSON(resultado4);
						html += '<label for="sector" class="select">Sector:</label><select name="sector" id="sector" onChange="NewZonaAlmacenz4()" data-theme="b"><option value="">Seleccione</option>';
						$.each(miRes4, function(i, elemento4){
							if (elemento.z3s == elemento4.sec) {
								html += '<option value="'+elemento4.sec+'" selected>'+elemento4.id+'</option>';
							} else {
								html += '<option value="'+elemento4.sec+'">'+elemento4.id+'</option>';
							}
						});
						jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n4",{"TK":clave,"pais":elemento.z1s,"est":elemento.z2s,"sector":elemento.z3s},function(resultado5){
							html+='</select>';
							miRes5 = jQuery.parseJSON(resultado5);
							html += '<label for="zona" class="select">Zona:</label><select name="zona" id="zona" data-theme="b"><option value="">Seleccione</option>';
							$.each(miRes5, function(i, elemento5){
								if (elemento.z4s == elemento5.sec) {
									html += '<option value="'+elemento5.sec+'" selected>'+elemento5.id+'</option>';
								} else {
									html += '<option value="'+elemento5.sec+'">'+elemento5.id+'</option>';
								}
							});
							html+='</select>';
							html += '<label for="desc">Dirección Detalle:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,300)">'+elemento.desc+'</textarea>'
							jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0001_consuta_tipo_zona",{"TK":clave},function(resultado6){
								miRes6 = jQuery.parseJSON(resultado6);
								html += '<label for="tipo" class="select">Tipo:</label><select name="tipo" id="tipo" data-theme="b"><option value="">Seleccione</option>';
								$.each(miRes6, function(i, elemento6){
									if (elemento.tipos == elemento6.sec) {
										html += '<option value="'+elemento6.sec+'" selected>'+elemento6.id+'</option>';
									} else {
										html += '<option value="'+elemento6.sec+'">'+elemento6.id+'</option>';
									}
								});
								html+='</select>';
								$(html).appendTo('#Resp');
								redireccionar('consultaU.html\#CResponsable');	
							});
						});
					});
				});
			});
		});
	});
}

function actualizarU() {
	select=document.getElementById("pais").options;
	index=document.getElementById("pais").selectedIndex;
	pais=select[index].value;
	select=document.getElementById("est").options;
	index=document.getElementById("est").selectedIndex;
	est=select[index].value;
	select=document.getElementById("sector").options;
	index=document.getElementById("sector").selectedIndex;
	sector=select[index].value;
	select=document.getElementById("zona").options;
	index=document.getElementById("zona").selectedIndex;
	zona=select[index].value;
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	tipo=select[index].value;
	desc=document.getElementById("desc").value;

	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_zona_0003_actualiza_ubicacion_detalle",{"TK":clave,"z4":zona,"z3":sector,"z2":est,"z1":pais,"desc":desc,"tipo1":tipo,"almacen":localStorage.almacen,"tipo":"almacen"},function(resultado){
		redireccionar("Almacen.html");
	});
}

function CResponsable(){
	//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0002_consulta_datos_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="ConsultaRespaux('+elemento.sec+',\''+elemento.nombre+'\','+elemento.tipo+',\''+elemento.activo+'\')"><h3>'+elemento.nombre+'</h3><p>Tipo: '+elemento.tipoid+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Responsables: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("CResponsable.html\#CResponsable");
	});	
}

function initResponsable(arg,ref){

	localStorage.ref=ref;
	localStorage.nom="";
	localStorage.tipo="";
	localStorage.resp="";
	localStorage.activo='S';
	redireccionar(arg);
	
}

//NO uso
function actnom(){
	localStorage.nom=document.getElementById("nombre").value;
}

//NO uso
function acttipo(){
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	localStorage.tipo=select[index].value;
}

function actcomp(){
	if(document.getElementById("comp").checked){localStorage.compuesto="S";}
	else{localStorage.compuesto="N";}
}

function actt(){
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	localStorage.t=select[index].value;
}

//NO uso
function actactivo(){
	if(document.getElementById("activo").checked){localStorage.activo="S";}
	else{localStorage.activo="N";}
}

function actaccliente() {
	if(document.getElementById("cliente").checked){localStorage.c="S";}
	else{localStorage.c="N";}
	if(document.getElementById("prov").checked){localStorage.p="S";}
	else{localStorage.p="N";}
	if(document.getElementById("benef").checked){localStorage.b="S";}
	else{localStorage.b="N";}
}

//Modificada 17/09/2012 Alvaro
function PreNewResp(){

	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_responsable_0001_consulta_tipo_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<label for="nombre">Nombre:</label><input type="text" name="nombre" id="nombre" value="'+localStorage.nom+'" maxlength="200"/>';
		html+='<div data-role="fieldcontain"><label for="tipo" class="select">Tipo:</label><select name="tipo" id="tipo" data-theme="b"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			if(localStorage.tipo==elemento.sec){
				html+='<option value="'+elemento.sec+'" selected>'+elemento.id+'</option>';
			}else{
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			}
		});
		if(localStorage.activo=="S"){
			html+='</select></div> <label for="activo">Activo </label> <input type="checkbox" name="activo" id="activo" value="S" checked/>';
		}else{
			html+='</select></div> <label for="activo">Activo </label> <input type="checkbox" name="activo" id="activo" value="S"/>';
		}
		
		if(localStorage.resp!=""){
			html+='<button onClick="ActResp()" data-theme = "b" align="center">Actualizar</button>';			
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_zona_0001_consulta_ubicacion",{"TK":clave,"almacen":localStorage.resp,"tipo":"responsable"},function(resultado1){
				miRes1 = jQuery.parseJSON(resultado1);
				html+='<div data-role="collapsible-set"><div data-role="collapsible" data-collapsed="true" data-theme = "b"><h3>Ubicación</h3><p><div data-role="content" id="lista" aling="center"><ul data-role="listview" data-inset="true" id="zona">';
				html+='<button onClick="redireccionar(\'NewZonaResp.html\')" align="center" data-theme = "b">Agregar Ubicación</button>';
				$.each(miRes1, function(i, elemento){
					html+='<li><h3>País:'+elemento.z4+', Estado/Ciudad: '+elemento.z2+', Sector: '+elemento.z3+', Zona: '+elemento.z4+'</h3><p>Tipo: '+elemento.tipo+'</p></li>';
				});
				html+='</ul></div></p></div>';	
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_mediocom_0001_consulta_mediocom_responsable",{"TK":clave,"resp":localStorage.resp},function(resultado2){
					miRes2 = jQuery.parseJSON(resultado2);
					html+='<div data-role="collapsible" data-collapsed="true" data-theme = "b"><h3>Medio de Comunicación</h3><p><div data-role="content" id="lista" aling="center"><ul data-role="listview" data-inset="true" id="mediocom">';
					html+='<button onClick="redireccionar(\'NewMedioCom.html\')" align="center" data-theme = "b">Agregar Medio de Comunicación</button>';
					$.each(miRes2, function(i, elemento){
						html+='<li><h3>'+elemento.id+': '+elemento.dato+'</h3></li>';					
					});
					html+='</ul></div></p></div></p>';	
					$(html).appendTo('#Resp');
					redireccionar("Responsable.html\#Responsable");	
				});
			});
		}else{
			html+='<button onClick="CrearResp()" data-theme="b">Crear Responsable</button>';	
			$(html).appendTo('#Resp');
			redireccionar("Responsable.html\#Responsable");
		}
	});	
}

//Modificada 17/09/2012 Alvaro
function CrearResp(){

	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	localStorage.tipo=select[index].value;
	
	localStorage.nom=document.getElementById("nombre").value;
	
	if(document.getElementById("activo").checked){localStorage.activo="S";}
	else{localStorage.activo="N";}
	
	if(localStorage.nom!="" && localStorage.tipo!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0003_insertar_responsable",{"TK":clave,"nombre":localStorage.nom,"tipo":localStorage.tipo,"user":localStorage.nombre,"activo":localStorage.activo},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			if(miRes.estado=="exito"){
				localStorage.resp=miRes.sec;
			}
			redireccionar("Responsable.html");
		});
	
	}else{alert("Error! Los campos Nombre y Tipo son obligatorios.");}
}

function ConsultaRespaux(sec,nom,tipo,activo){
	localStorage.resp=sec;
	localStorage.nom=nom;
	localStorage.tipo=tipo;
	localStorage.activo=activo;
	localStorage.ref="CResponsable.html";
	redireccionar("Responsable.html");
}

//Modificada 17/09/2012 Alvaro
function ActResp(){

	localStorage.nom=document.getElementById("nombre").value;

	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	localStorage.tipo=select[index].value;

	if(document.getElementById("activo").checked){localStorage.activo="S";}
	else{localStorage.activo="N";}
	
	if(localStorage.nom!="" && localStorage.tipo!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0004_actualiza_responsable",{"TK":clave,"nom":localStorage.nom,"tipo":localStorage.tipo,"user":localStorage.nombre,"sec":localStorage.resp},function(resultado){
			redireccionar("Responsable.html");
		});
	}else{alert("Error! El campo Nombre es obligatorio.");}
}

function PreNewZonaAlmacen(){
	localStorage.pais="";
	localStorage.est="";
	localStorage.sector="";
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0001_consuta_tipo_zona",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			html='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		$(tipo).empty;
		$(tipo).html(html).selectmenu('refresh',true);
	
	});
}

function NewZonaAlmacenz1(){

	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv1_0001_consulta_zona_n1",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		$('#pais').empty();
		$('#pais').html(html).selectmenu('refresh',true);
	
	});
}

function NewZonaAlmacenz2(){
	
	select=document.getElementById("pais").options;
	index=document.getElementById("pais").selectedIndex;
	localStorage.pais=select[index].value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n2",{"TK":clave,"pais":localStorage.pais},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		$(est).empty;
		$(est).html(html).selectmenu('refresh',true);
		html='<option value="">Seleccione</option>';
		$(sector).empty;
		$(sector).html(html).selectmenu('refresh',true);
		$(zona).empty;
		$(zona).html(html).selectmenu('refresh',true);
	
	});
}

function NewZonaAlmacenz3(){
	
	select=document.getElementById("est").options;
	index=document.getElementById("est").selectedIndex;
	localStorage.est=select[index].value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n3",{"TK":clave,"pais":localStorage.pais,"est":localStorage.est},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		$(sector).empty;
		$(sector).html(html).selectmenu('refresh',true);
		html='<option value="">Seleccione</option>';
		$(zona).empty;
		$(zona).html(html).selectmenu('refresh',true);
	
	});
}

function NewZonaAlmacenz4(){
	
	select=document.getElementById("sector").options;
	index=document.getElementById("sector").selectedIndex;
	localStorage.sector=select[index].value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_zona_niv2_0001_consulta_zona_n4",{"TK":clave,"pais":localStorage.pais,"est":localStorage.est,"sector":localStorage.sector},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		$(zona).empty;
		$(zona).html(html).selectmenu('refresh',true);
	
	});
}

function AgregarZona(AorR){

	if(localStorage.pais!="" && localStorage.est!="" && localStorage.sector!=""){
		
		select=document.getElementById("zona").options;
		index=document.getElementById("zona").selectedIndex;
		zona=select[index].value;
		
		select=document.getElementById("tipo").options;
		index=document.getElementById("tipo").selectedIndex;
		tipo=select[index].value;
		
		desc=document.getElementById("textarea").value;
		
		if(zona!="" && tipo!=""){
			if(AorR=="Almacen"){sec=localStorage.almacen;arg="Almacen.html";}
			else{sec=localStorage.resp;arg="Responsable.html";}
			jQuery.get("http://"+IP+":"+puerto+"/AgregarZona",{"TK":clave,"pais":localStorage.pais,"est":localStorage.est,"sector":localStorage.sector,"zona":zona,"tipo":tipo,"desc":desc,"user":localStorage.nombre,"alm":sec,"AorR":AorR},function(resultado){
				redireccionar(arg);
			});
		
		}
	}

}

//Función auxiliar que inicializa las variables necesarias para realizar una consulta filtrada por un rango de fechas
//Cambiar Nombre
function PreConsultaRecepcion(arg){	
	localStorage.FI="";
	localStorage.FF="";
	localStorage.cod = "";
	localStorage.t = "";
	redireccionar(arg);
}

function ConsultaC(sec){
	localStorage.Codif=sec;
	redireccionar("CCodificacionE.html");
}

//Función que genera la pantalla de consultar una recepción seleccionada
function ConsultaCodificacion(){
	//Servicio que busca los datos de la codificación seleccionada
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0012_consulta_codificacion",{"TK":clave,"sec":localStorage.Codif},function(resultado){
		miRes = jQuery.parseJSON(resultado);	
		html = "";
		$.each(miRes, function(i, elemento){
			html+='<div data-role="fieldcontain"><label for="textinput2">Identificador:</label><input type="text" name="textinput2" id="textinput2" value="'+elemento.id+'" readonly  /> </div>';
			html+='<div data-role="fieldcontain"><label for="textarea">Descripción:</label><textarea cols="40" rows="8" name="textarea" id="textarea" readonly>'+elemento.desc+'</textarea></div>';
			html+='<div data-role="fieldcontain"><label for="textinput7">Fecha:</label><input type="text" name="textinput7" id="textinput7" value="'+elemento.fecha+'" readonly /> </div>';
			html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><input type="text" name="textinput5" id="textinput5" value="'+elemento.resp+'" readonly /></div>';
			if (elemento.sum == 'None') {
				html+='<div data-role="fieldcontain"><label for="textinput">Nro. de items:</label><input type="text" name="textinput" id="textinput" value="0" readonly /></div>';
			} else {
				html+='<div data-role="fieldcontain"><label for="textinput">Nro. de items:</label><input type="text" name="textinput" id="textinput" value="'+elemento.sum+'" readonly /></div>';	
			}
			localStorage.mov=localStorage.Codif;
			
		});
		html+='</ul></div>';
		html+='<div align="center" data-role="controlgroup" data-type="horizontal"><button data-theme="b" onClick="redireccionar(\'Codificacion.html\')">Continuar Codificación</button><button button data-inline="true" data-theme="b" onClick="CerrarCod()">Cerrar Codificación</button></div>';

		$(html).appendTo('#CCodif');
		redireccionar("CCodificacionE.html\#CCodificacionE");
	});
}

function NewAlmacen(){
	localStorage.id=document.getElementById("id").value;
	localStorage.desc=document.getElementById("desc").value;
	tipof=document.getElementById("radio1_0");
	tipov=document.getElementById("radio1_1");
	if(tipof.checked){localStorage.tipo="F";}
	else if (tipov.checked){localStorage.tipo="V";}
	if(document.getElementById("activo").checked){localStorage.activo="S";}
	else{localStorage.activo="N";}
	select=document.getElementById("res").options;
	index=document.getElementById("res").selectedIndex;
	if(index!=0){
		localStorage.resp=select[index].value;
	}
	if(localStorage.resp!="" && localStorage.id!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0004_insertar_almacen",{"TK":clave,"id":localStorage.id,"desc":localStorage.desc,"tipo":localStorage.tipo,"activo":localStorage.activo,"resp":localStorage.resp,"user":localStorage.nombre},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			$.each(miRes, function(i, elemento){
				localStorage.almacen=elemento.almacen;
			});
			redireccionar("Almacen.html");			
		});
	}else{
		alert("Error! Los campos Identificador y Responsable son obligatorios.");
	}
}


function NewMedioCom(){
	//jQuery.get("http://"+IP+":"+puerto+"/CMC",function(resultado){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_mediocom_0001_consulta_medios_comunicacion_activos",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<div data-role="fieldcontain"><label for="tipo" class="select">Tipo:</label><select name="tipo" id="tipo" data-theme="b"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		html+='</select></div><label for="dato">Dato:</label><input type="text" name="dato" id="dato" maxlength="200"/><button onClick="AgregarMC()" data-theme = "b">Agregar</button>';
		$(html).appendTo('#MC');
		redireccionar("NewMedioCom.html\#RespMedioC");
	});
}
/*function NewMedioCom(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_mediocom_0001_consulta_medios_comunicacion_activos",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<div data-role="fieldcontain"><label for="tipo" class="select">Tipo:</label><select name="tipo" id="tipo" data-theme="b"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		html+='</select></div><label for="dato">Dato:</label><input type="text" name="dato" id="dato"/><button onClick="AgregarMC()" data-theme = "b">Agregar</button>';
		$(html).appendTo('#MC');
		redireccionar("NewMedioCom.html\#RespMedioC");
	});
}*/
function AgregarMC(){
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	tipo=select[index].value;
	dato=document.getElementById("dato").value;

	if(dato!="" && tipo!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_mediocom_0001_insertar_mediocom_responsable",{"TK":clave,"resp":localStorage.resp,"dato":dato,"tipo":tipo,"user":localStorage.nombre},function(resultado){
			redireccionar("Responsable.html");
		});
	}else{alert("Error! Los campos ");}
}
/*
function AgregarMC(){
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	tipo=select[index].value;
	alert(tipo);
	dato=document.getElementById("dato").value;

	if(dato!="" && tipo!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_mediocom_0001_insertar_mediocom_responsable",{"TK":clave,"resp":localStorage.resp,"tipo":tipo,"dato":dato,"user":localStorage.nombre},function(resultado){
			redireccionar("Responsable.html");
		});
	}else{alert("Faltan Parametros");}

}*/

//Función que almacena los valores de las fechas a utilizar en el flitro de la busqueda
function GuardarFecha(arg){
	
	localStorage.FI=document.getElementById('FI').value;
	localStorage.FF=document.getElementById('FF').value;
	if(localStorage.FI==""){
		alert("Debe ingresar al menos la Fecha Desde la cual desa consultar la operación.");
	}else{
		redireccionar(arg);
	}
}

//Función que almacena los valores de las fechas a utilizar en el flitro de la busqueda
function GuardarDatos(arg){
	localStorage.cod=document.getElementById('cod').value;
	localStorage.t=document.getElementById('tipo').value;
	redireccionar(arg);
}

//Función auxiliar que inicializa las variables necesarias para realizar una consulta filtrada por un rango de fechas
//Cambiar Nombre
function PreConsultaRecepcion(arg){
	
	localStorage.FI="";
	localStorage.FF="";
	localStorage.cod="";
	localStorage.t="";
	redireccionar(arg);
}

// Cambia el estatus de la codificación a cerrado
function CerrarCod() {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0013_modifica_cierra_codificacion",{"TK":clave,"sec":localStorage.Codif,"user":localStorage.nombre},function(resultado){
		redireccionar("CCodificacion.html");
	});
}
/*********************************************************************************************************************************/
/*****                                                 FUNCIONES - María Toledo                                              *****/
/*****                                                 		  MÓDULO BASE    												 *****/
/*********************************************************************************************************************************/
/* Abre una nueva ventana en la página de SID
 * 20120613
 */
function sid() {
	window.open("http://www.integradores.net");		
}

/* Limpia las variables del localStorage, y redirecciona al Login
 * 20120613
 */
function salir() {
	localStorage.clear();
	window.open("index.html","_self");	
}

/* Función que redirecciona al url pasapa por parámetro
 * 20120613
 */
function redireccionar(url) {
	localStorage.regresar = url;
	window.open(url,"_self");
}

function redireccionarC(url) {
	localStorage.regresar = url;
	localStorage.x = "";
	localStorage.y = ""; 
	window.open(url,"_self");
}


/* Realiza la validación de los datos del login
 * 20120911 
 */
function login(usuario, pswd, clave) {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_seg_users_0001_consulta_login",{"TK":clave,"login":usuario,"pswd":pswd},function(resultado){
		if (resultado == "fracaso"){
			alert("Usuario no autorizado!")	
		} else if (resultado == "inactivo"){
			alert("El usuario " + usuario + " se encuentra inactivo.")	
		} else {
			respuesta = jQuery.parseJSON(resultado);
			localStorage.nombre = usuario;
			localStorage.activo = 'S'; localStorage.c = 'S'; localStorage.p = 'S';
			localStorage.b = 'S'; localStorage.x = ""; localStorage.y = "";
			location.href = "index.html\#menu_ppal";
		}
	})
}

function Consulta(sec) {
	localStorage.Inv=sec;
	localStorage.mov=sec;
	redireccionar("CInventarioE.html");
}

/********************************************************************************************************************************************/
/***                                                     PANTALLAS INVENTARIO INICIAL                                                     ***/
/********************************************************************************************************************************************/
/* Consulta los Inventarios Iniciales dependiendo del Filtro indicado por el usuario
 * 201209112
 */
function CInventario(){
	$('#FI').val(localStorage.FI);
	$('#FF').val(localStorage.FF);
	if(localStorage.FI != "") {
		if(localStorage.FF != "") {
			fin=localStorage.FF;
		} else {
			d = new Date();
			mes = d.getMonth()+1;
			fin = d.getFullYear()+"-"+mes+"-"+d.getDate();
		}
		
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0007_consulta_inventario_inicia",{"TK":clave,"FI":localStorage.FI,"FF":fin,"sec":0},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			$.each(miRes, function(i, elemento){
				nro+=1;
				html+='<li><a onClick="Consulta('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.resp+'; Almacén: '+elemento.alm+'</p><p>Fecha: '+elemento.fecha+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaRec"><li data-role="list-divider" role="heading"><h3>Inventarios Iniciales En Proceso: '+nro+'</h3></li>'+html+'</ul>';
			$(html).appendTo('#Recep');
			redireccionar("CInventario.html\#CRecepcion");
		});	
	} else {
		//t_cor_inv_movimiento_lote_0009_consulta_inventario_inicial
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0009_consulta_inventario_inicial",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			var est = '';
			$.each(miRes, function(i, elemento){
				if (elemento.estatus = 'S') { 
					est = "Sin Procesar";
				} else {
					est = "Procesado"
				}
				nro+=1;
				html+='<li><a onClick="Consulta('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.resp+'; Almacén: '+elemento.alm+'</p><p>Fecha: '+elemento.fecha+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaRec"><li data-role="list-divider" role="heading"><h3>Inventarios Iniciales En Proceso: '+nro+'</h3></li>'+html+'</ul>';
			$(html).appendTo('#Recep');
			redireccionar("CInventario.html\#CRecepcion");
		});
	}
}

/* Inicializa las variables necesarias para ingresar un nuevo Inventario Inicial
 * 201209112
 */
function PreIngresarInvInicial(){	
	localStorage.mov="";
	localStorage.matcat="";
	localStorage.almacen="";
	localStorage.monto="";
	redireccionar('InventarioInicial.html');
}


/* Carga los datos de necesarios para crear un Inventario Inicial
 * 201209112
 */
function PreIngresarRecepInv(){
	d = new Date();
	mes = d.getMonth() + 1;
	fecha_actual = d.getFullYear() + '-' +  mes + '-' + d.getDate();
	if(localStorage.mov==""){	
		html='<div data-role="fieldcontain"><label for="textinput2">Identificador:</label><input type="text" name="textinput2" id="textinput2" value="" maxlength="32"/> </div>';
		html+='<div data-role="fieldcontain"><label for="textarea">Descripción:</label><textarea cols="40" rows="8" name="textarea" id="textarea" onKeyUp="return maximaLongitud(this,100)"></textarea></div>';
		html+='<div data-role="fieldcontain"><label for="FI">Fecha:</label><input type="text" name="FI" id="FI" placeholder="yyyy-mm-dd" value="'+fecha_actual+'" maxlength="10"/></div>';
		// Se obtiene los Responsables
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0001_consulta_responsable",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			html+='<div data-role="fieldcontain"><label for="resp">Responsable:</label><select data-theme="b" name="resp" id="resp" data-theme="b"> <option value="">Seleccione</option>';
			$.each(miRes, function(i, elemento){
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			});
			html+='</select></div>';
			// Se obtienen los almacenes
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0001_consulta_almacen",{"TK":clave},function(resultado){
				miRes = jQuery.parseJSON(resultado);
				html+='<div data-role="fieldcontain"><label for="alm">Almacén:</label><select data-theme="b" name="alm" id="alm" data-theme="b"><option value="">Seleccione</option>';
				$.each(miRes, function(i, elemento){
					html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
				});
				html+='</select></div>';
				html+='<button data-theme="b" onClick="IngresarInvInicial()">Crear Inventario Inicial</button>';
				$(html).appendTo('#DRecepcion');
				redireccionar("InventarioInicial.html\#NewRecep");
			});
		});
	}
}

/* Ingresa los datos de Inventario Inicial
 * 201209112
 */
function IngresarInvInicial(){
	select = document.getElementById("resp").options;
	index = document.getElementById("resp").selectedIndex;
	resp=select[index].value;
	select = document.getElementById("alm").options;
	index = document.getElementById("alm").selectedIndex;
	alm=select[index].value;
	id=document.getElementById("textinput2").value;
	desc=document.getElementById("textarea").value;
	fecha=document.getElementById("FI").value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0005_insertar_inventario_inicial",{"TK":clave,"id":id,"des":desc,"fecha":fecha,"resp":resp,"user":localStorage.nombre,"alm":alm},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		redireccionar('CInventario.html');
	});	
}

/* Carga los datos de la pantalla de Consulta de un Inventario Inicial
 * 2012-09-13
 */
function ConsultaInv(){
	var estatus = '';
	var est = '';
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0008_consulta_inventario_inicia",{"TK":clave,"sec":localStorage.Inv},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html = "";
		$.each(miRes, function(i, elemento){
			estatus = elemento.estatus;
			if (estatus = 'S') {
				est = "Sin Procesar";
			} else if (estatus = 'P') {
				est = "Procesado";
			}
	 		html+='<div data-role="fieldcontain"><label for="textinput2">Inventario Inicial:</label><input type="text" name="textinput2" id="textinput2" value="'+elemento.id+'" readonly  /> </div>';
			html+='<div data-role="fieldcontain"><label for="textinput6">Estatus:</label><input type="text" name="textinput6" id="textinput6" value="'+est+'"  readonly/></div>';
			html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><input type="text" name="textinput5" id="textinput5" value="'+elemento.resp+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput4">Almacén:</label><input type="text" name="textinput4" id="textinput4" value="'+elemento.alm+'" readonly /></div>';	
		});
		
		// Se consultan los items cargados al Inventario
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0001_consulta_inventario_inicial",{"TK":clave,"mov":localStorage.Inv},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			items = 0;
			html2 = '';
			$.each(miRes, function(i, elemento){
				nro+=1;
				html2+='<li><h3>'+elemento.id+'</h3><p>Cantidad: '+elemento.cant+'</p></li>';
				items+=parseInt(elemento.cant);
			});
			
			if(estatus !='P'){			
				html+='<div align="center" data-role="controlgroup" data-type="horizontal"><button button data-inline="true" data-theme="b" onClick="redireccionar(\'NewInv.html\')">Continuar Inventario</button><button button data-inline="true" data-theme="b" onClick="CerrarInv()">Cerrar Inventario</button></div>';
			}
			
			html2='<ul data-role="listview" data-inset="true" id="ConsultaRec"><li data-role="list-divider" role="heading">Nro. de Materiales Registrados: '+nro+'; Nro. de Items Registrados: '+items+'</li>'+html2+'</ul>';
			html+=html2;
			$(html).appendTo('#CRecep');
			redireccionar("CInventarioE.html\#CRecepcionE");
		});
	});
}

/* Carga los datos de la pantalla que almacena los items de un Inventario Inicial
 * 2012-09-13
 */
function PreIngresarInvDetalle(){
	if(localStorage.mov==""){
			redireccionar("NewInv.html\#NewRecep");
	}else{
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_matcat",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			html = "";
			html+='<label for="mat">Material:</label><select name="mat" id="mat" data-theme="b"><option value="">Seleccione</option>';
			$.each(miRes, function(i, elemento){
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			});
		
			html+='</select>';
			html+='<label for="cant">Cantidad:</label><input type="number" name="cant" id="cant" value="0" />';	
			html+='<button data-theme="b" onClick="GuardarMaterial()">Procesar Item</button>';
			$(html).appendTo('#DRecep');
			redireccionar("NewInv.html\#NewRecep");
		});
	}
}

/* Guarda en BD los items de un Inventario Inicial
 * 2012-09-13
 */
function GuardarMaterial() {
	select=document.getElementById("mat").options;
	index=document.getElementById("mat").selectedIndex;
	material=select[index].value;
	cantidad = document.getElementById("cant").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0002_insertar_inventario_detalles",{"TK":clave,"mov":localStorage.Inv,"matcat":material,"cant":cantidad,"user":localStorage.nombre},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		redireccionar("NewInv.html");
	});
}

/* Actualiza el estatus de un Inventario Inicial a Procesado
 * 2012-09-13
 */
function CerrarInv(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0010_modifica_inventario_inicial",{"TK":clave,"sec":localStorage.Inv,"user":localStorage.nombre},function(resultado1){
		redireccionar("CInventario.html");
	});
}

//Función auxiliar que almacena los datos necesarios para consultar una recepción
function InventarioR(sec){
	localStorage.Inv=sec;
	redireccionar("CInventarioE.html");
}
/********************************************************************************************************************************************/
/***                                                          PANTALLAS CODIFICACION                                                      ***/
/********************************************************************************************************************************************/
function CCodificacion(){
	$('#FI').val(localStorage.FI);
	$('#FF').val(localStorage.FF);
	var fin;
	if(localStorage.FI != ""){
		if(localStorage.FF != ""){
			fin=localStorage.FF;
		}else{
			d = new Date();
			mes = d.getMonth()+1;
			fin = d.getFullYear()+"-"+mes+"-"+d.getDate();
		}
		
		//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0002_consulta_codificacion",{"TK":clave,"FI":localStorage.FI,"FF":fin},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			$.each(miRes, function(i, elemento){
				nro+=1;
				html+='<li><a onClick="ConsultaC('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.resp+'; Fecha: '+elemento.fecha+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Codificaciones: '+nro+'</h3></li>'+html+'</ul>';
			$(html).appendTo('#Codif');
			redireccionar("CCodificacion.html\#CCodificacion");
		});	
	}else{		
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0001_consulta_codificacion",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			$.each(miRes, function(i, elemento){
				nro+=1;
				html+='<li><a onClick="ConsultaC('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.resp+'; Fecha: '+elemento.fecha+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Codificaciones: '+nro+'</h3></li>'+html+'</ul>';
			$(html).appendTo('#Codif');
			redireccionar("CCodificacion.html\#CCodificacion");
		});
	}
}

//Función que genera la pantalla para ingresar una nueva codificación
function PreCod(){
	d = new Date();
	mes = d.getMonth() + 1;
	fecha_actual = d.getFullYear() + '-' +  mes + '-' + d.getDate();
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0001_consulta_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<div data-role="fieldcontain"><label for="id">Identificador:</label><input type="text" name="id" id="id" maxlength="32"/></div>';
		html+='<div data-role="fieldcontain"><label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea></div>';
		html+='<div data-role="fieldcontain"><label for="fecha">Fecha:</label><input type="text" name="fecha" id="fecha" placeholder="yyyy-mm-dd" value="'+fecha_actual+'" maxlength="10"/></div>';
		html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><select data-theme="b" name="textinput5" id="textinput5" data-theme="b"> <option value="">Seleccione</option>';

		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		html+='</select></div>';
		html+='<button data-inset="true" data-theme="b" onClick="MenuCodif()">Crear Codificación</button>';
		$(html).appendTo('#Cod');
		redireccionar("PreCod.html\#PreCod");
	});
}


//Función que inicializa todos los datos necesarios y crea el encabezado de una codificación
function MenuCodif(){
	localStorage.matcat="";
	localStorage.almacen="";
	localStorage.mov="";
	id=document.getElementById("id").value;
	desc=document.getElementById("desc").value;
	fecha=document.getElementById("fecha").value;
	select = document.getElementById("textinput5").options;
	index = document.getElementById("textinput5").selectedIndex;
	resp=select[index].value;

	if(id!="" && fecha!="" && resp!=""){
		//Servicio que genera el encabezado de una codificación
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0003_insertar_codificacion",{"TK":clave,"id":id,"desc":desc,"fecha":fecha,"resp":resp,"user":localStorage.nombre},function(resultado){
			$.each(miRes, function(i, elemento){
				localStorage.mov=elemento.sec;
				redireccionar('CCodificacion.html');
			});
		});
	}else{
		alert("Error! Los campos Identificador, Fecha, y Responsable son obligatorios.");
	}
}


//Función que genera la pantalla que realiza las codificaciones
function PreCodificacion(){
	// Se obtienen la lista de items por codificar
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0004_consultar_matcat_codificacion",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<div data-role="fieldcontain"><label for="matcat">Material a Codificar:</label><select data-theme="b" name="matcat" id="matcat" onChange="ActDatos()" > <option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){			
			html+='<option value="'+elemento.sec+'">'+elemento.id+' ('+elemento.sum+')</option>';
		});
		// Se obtienen la lista de almacenes destino
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0002_consulta_almacen_activo",{"TK":clave},function(resultado1){
			miRes1 = jQuery.parseJSON(resultado1);
			almacen='<div data-role="fieldcontain"><label for="almacen">Almacén Destino:</label><select data-theme="b" name="almacen" id="almacen" onChange="ActAlm()"> <option value="">Seleccione</option>';
			$.each(miRes1, function(i, elemento1){
				almacen+='<option value="'+elemento1.sec+'">'+elemento1.id+'</option>';
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0005_consultar_detalle_codificacion",{"TK":clave,"mov":localStorage.mov},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				nro = 0;
				items = 0;
				lista = '<div data-role="fieldcontain"><label for="CI">Código Inicial:</label><input type="number" name="CI" id="CI" value="" min="0"/></div>';
				lista += '<div data-role="fieldcontain"><label for="CF">Cantidad:</label><input type="number" name="CF" id="CF" value="" min="1"/></div>';
				lista += '<button data-theme="b" onClick="GenerarCod()">Procesar Códigos</button><br/><ul data-role="listview" data-inset="true">';
				lista2 = '';
				$.each(miRes2, function(i, elemento2){
					lista2+='<li><h3>'+elemento2.id+'</h3><p>Nro. de items codificados: '+elemento2.cant+'</p></li>';
					nro+=1;
					items+=parseInt(elemento2.cant);
				});
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0001_consulta_colores_activos",{"TK":clave},function(resultado3){
					miRes3 = jQuery.parseJSON(resultado3);
					color='<div data-role="fieldcontain"><label for="color">Color:</label><select data-theme="b" name="color" id="color" onChange="ActColor()"><option value="">Seleccione</option>';
					$.each(miRes3, function(i, elemento3){
						color+='<option value="'+elemento3.sec+'">'+elemento3.id+'</option>';
					});
					jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0001_consulta_materia_prima_activa",{"TK":clave},function(resultado4){
						miRes4 = jQuery.parseJSON(resultado4);
						materiap='<div data-role="fieldcontain"><label for="matp">Materia Prima:</label><select data-theme="b" name="matp" id="matp" onChange="ActMatP()"><option value="">Seleccione</option>';
						$.each(miRes4, function(i, elemento4){
							materiap+='<option value="'+elemento4.sec+'">'+elemento4.id+'</option>';
						});
						materiap+='</select></div>';
						color+='</select></div>';
						lista+='<li data-role="list-divider" role="heading" data-theme="b">Materiales Codificados: '+nro+'; Nro. de artículos codificados: '+items+'</li>' + lista2 + '</ul>';
						almacen+='</select></div>';
						html+='</select></div> ' + almacen + ' ' + color + ' ' + materiap + ' ' + lista;
						$(html).appendTo('#cod');
						redireccionar("Codificacion.html\#Cod");
					});
				});
			});
		});
	});	
}

//Función auxiliar que almacena el valor del material seleccionado y recarga la pantalla
function ActDatos(){
	select = document.getElementById("matcat").options;
	index = document.getElementById("matcat").selectedIndex;
	localStorage.matcat=select[index].value;
}

//Función auxiliar que guarda el valor del almacen seleccionado 
function ActAlm(){
	select = document.getElementById("almacen").options;
	index = document.getElementById("almacen").selectedIndex;
	localStorage.almacen=select[index].value;
	
}

//Función auxiliar que guarda el valor del color seleccionado 
function ActColor(){
	select = document.getElementById("color").options;
	index = document.getElementById("color").selectedIndex;
	localStorage.color=select[index].value;	
}

//Función auxiliar que guarda el valor de la materia prima seleccionado 
function ActMatP(){
	select = document.getElementById("matp").options;
	index = document.getElementById("matp").selectedIndex;
	localStorage.matp=select[index].value;	
}


function ActMater(){
	select = document.getElementById("matcat1").options;
	index = document.getElementById("matcat1").selectedIndex;
	localStorage.x=select[index].value;	
}

function ActAlma(){
	select = document.getElementById("alm").options;
	index = document.getElementById("alm").selectedIndex;
	localStorage.y=select[index].value;	
}

//Función que Genera los articulos de una codificación 
function GenerarCod(){
	CI=document.getElementById("CI").value;
	Cantidad=document.getElementById("CF").value;
	select = document.getElementById("almacen").options;
	index = document.getElementById("almacen").selectedIndex;
	almacen=select[index].value;
	select = document.getElementById("matcat").options;
	index = document.getElementById("matcat").selectedIndex;
	matcat=select[index].value;
	// Se Verifica que los codigo no se encuentren registrados
	
	if (CI == "" || almacen == "" || matcat == "" || Cantidad == "") {
		alert("Error! Los campos Material a Codificar, Almacén Destino, Código Inicial, y Cantidad son obligatorios.");
	} else {
		CI = parseInt(document.getElementById("CI").value);
		CF = CI + parseInt(document.getElementById("CF").value) - 1;
		dif = CF - CI + 1;
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0001_consulta_verificar_codigos",{"TK":clave,"CI":CI,"CF":CF},function(resultado){
			var error = '';
			miRes = jQuery.parseJSON(resultado);
			$.each(miRes, function(i, elemento){
				error+=elemento.sec+', ';
			});
			if (error == ''){
				// Se verifica que el número de ariculos que genero no exceda el número de materiales disponibles
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0006_consultar_por_codificar",{"TK":clave,"matcat":localStorage.matcat},function(resultado){
					var xc;
					miRes = jQuery.parseJSON(resultado);
					$.each(miRes, function(i, elemento){
						xc=elemento.xcod;
					});
					if (dif > xc) {
						alert('Error! Se intentan registrar ' + dif + ' artículos, y sólo se encuentran disponibles para codificar ' + xc + ' artículos.');	
						redireccionar("Codificacion.html");
					} else {
						//Servicio que genera los articulos, crea el detalle de la codificación y actualiza las recepciones				
						jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0007_generar_codificacion",{"TK":clave,"matcat":localStorage.matcat,"CI":CI,"CF":CF,"almacen":localStorage.almacen,"user":localStorage.nombre,"mov":localStorage.mov,"color":localStorage.color,"matprima":localStorage.matp},function(resultado){
							 redireccionar('Codificacion.html');
						});
					}
				});
			} else {
				alert('Error! Los siguientes Códigos ya se encuentran registrados: ' + error);	
				//redireccionar("Codificacion.html");
				return
			}
		});
	}
}

/********************************************************************************************************************************************/
/***                                                          PANTALLAS DESPACHO                                                          ***/
/********************************************************************************************************************************************/
function CMovimiento(arg){
	localStorage.tipo=arg[0];
	$('#FI').val(localStorage.FI);
	$('#FF').val(localStorage.FF);
	
	if(localStorage.FF != ""){
		fin=localStorage.FF;
	} else {
		d = new Date();
		mes = d.getMonth()+1;
		fin = d.getFullYear()+"-"+mes+"-"+d.getDate();
	}
	
	//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_0011_consulta_movimientos",{"TK":clave,"FI":localStorage.FI,"FF":fin,"tipo":localStorage.tipo},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="ConsultaM('+elemento.sec+',\'C'+arg+'E.html\',\''+localStorage.tipo+'\')"><h3>'+elemento.id+'</h3><p>Responsable: '+elemento.nombre+'; Fecha: '+elemento.fecha+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Movimientos: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#'+arg);
		redireccionar("C"+arg+".html\#C"+arg);
	});	
}

//Consulta Despacho,Traslado e Inventario
function ConsultaM(sec,arg,tipo){

	localStorage.ref=arg;
	localStorage.estatus="";
	localStorage.tipo=tipo;
	localStorage.mov=sec;
	localStorage.radio="I";
	localStorage.comp="";
	localStorage.emp="";
	localStorage.almacen="";
	localStorage.cant=0;
	localStorage.id="";
	localStorage.desc="";
	localStorage.fecha="";
	localStorage.resp="";
	redireccionar(arg);
}


function PreNewDespacho(){
	d = new Date();
	mes = d.getMonth()+1;
	fin = d.getFullYear()+"-"+mes+"-"+d.getDate();
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0001_consulta_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<label for="id">Identificador:</label><input type="text" name="id" id="id" value="'+localStorage.id+'" maxlength="32"/>';
		html+='<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+localStorage.desc+'</textarea>';
		if (localStorage.fecha == "") {
			html+='<label for="fecha">Fecha:</label><input type="text" name="fecha" id="fecha" placeholder="yyyy-mm-dd" value="'+fin+'" maxlength="10"/>';
		} else {
			html+='<label for="fecha">Fecha:</label><input type="text" name="fecha" id="fecha" placeholder="yyyy-mm-dd" value="'+localStorage.fecha+'" maxlength="10"/>';
		}
		html+='<label for="resp">Responsable:</label><select data-theme="b" name="resp" id="resp"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'" >'+elemento.id+'</option>';
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0002_consulta_almacen_activo",{"TK":clave},function(resultado1){
			miRes1 = jQuery.parseJSON(resultado1);
			html+='</select><label for="alm">Almacén:</label><select data-theme="b" name="alm" id="alm"><option value="">Seleccione</option>';
			$.each(miRes1, function(i, elemento1){
				html+='<option value="'+elemento1.sec+'" >'+elemento1.id+'</option>';
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0001_consulta_composicion_activo",{"TK":clave},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				html+='</select><label for="comp">Composición:</label><select data-theme="b" name="comp" id="comp" ><option value="">Seleccione</option>';
				$.each(miRes2, function(i, elemento2){
					html+='<option value="'+elemento2.sec+'" >'+elemento2.id+'</option>';
				});
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_0001_consulta_beneficiario_activo",{"TK":clave},function(resultado3){
					miRes3 = jQuery.parseJSON(resultado3);
					html+='</select><label for="emp">Beneficiario:</label><select data-theme="b" name="emp" id="emp" ><option value="">Seleccione</option>';
					$.each(miRes3, function(i, elemento3){
						html+='<option value="'+elemento3.sec+'" >'+elemento3.id+'</option>';
					});
					
					html+='</select><label for="cant">Cantidad:</label><input type="number" name="cant" id="cant" value="'+localStorage.cant+'" min="0"/>';
					html+='<button data-theme="b" onClick="NewDespacho()">Crear Despacho</button>';
					$(html).appendTo('#Despacho');
					redireccionar('NewDespacho.html\#NewDespacho');
				});
			});
		});
	});
}


function NewDespacho(){
	ActualizarCon(false);
	
	if(localStorage.id!="" && localStorage.fecha!="" && localStorage.resp!="" && localStorage.emp!="" && localStorage.comp!="" && localStorage.cant!="" && localStorage.almac!=""){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_0001_inserta_despacho",{"TK":clave,"id":localStorage.id,"desc":localStorage.desc,"fecha":localStorage.fecha,"resp":localStorage.resp,"emp":localStorage.emp,"alm":localStorage.almac,"comp":localStorage.comp,"cant":localStorage.cant,"user":localStorage.nombre},function(resultado){
			
			redireccionar("CDespacho.html");
		});
	} else { 
		alert("Error! Los campos Identificador, Fecha, Responsable, Almacén, Comosición, Beneficiario, y Cantidad, son obligatorios.");
	}
}


function ActualizarCon(arg){
	localStorage.id=document.getElementById("id").value;
	localStorage.fecha=document.getElementById("fecha").value;
	localStorage.desc=document.getElementById("desc").value;
	
	select=document.getElementById("resp").options;
	index=document.getElementById("resp").selectedIndex;
	if(index!=0){
		localStorage.resp=select[index].value;
	}
	localStorage.cant=document.getElementById("cant").value;	
	
	select=document.getElementById("emp").options;
	index=document.getElementById("emp").selectedIndex;
	if(index!=0){
		localStorage.emp=select[index].value;
	}
	select=document.getElementById("comp").options;
	index=document.getElementById("comp").selectedIndex;
	if(index!=0){
		localStorage.comp=select[index].value;
	}
	select=document.getElementById("alm").options;
	index=document.getElementById("alm").selectedIndex;
	if(index!=0){
		localStorage.almac=select[index].value;
	}
	if(arg){redireccionar("NewDespacho.html");}
}

//Función que genera la pantalla de consultar una recepción seleccionada
function ConsultaDespacho(){

	//Servicio que busca los datos de la codificación seleccionada
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_0002_consulta_despacho",{"TK":clave,"sec":localStorage.mov},function(resultado){
		miRes = jQuery.parseJSON(resultado);	
		html = "";
		$.each(miRes, function(i, elemento){
		
			html+='<div data-role="fieldcontain"><label for="textinput2">Identificador:</label><input type="text" name="textinput2" id="textinput2" value="'+elemento.id+'" readonly  /> </div>';
			html+='<div data-role="fieldcontain"><label for="textarea">Descripción:</label><textarea cols="40" rows="8" name="textarea" id="textarea" readonly>'+elemento.desc+'</textarea></div>';
			html+='<div data-role="fieldcontain"><label for="textinput7">Fecha:</label><input type="text" name="textinput7" id="textinput7" value="'+elemento.fecha+'" readonly /> </div>';
			html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><input type="text" name="textinput5" id="textinput5" value="'+elemento.resp_n+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput">Beneficiario:</label><input type="text" name="textinput" id="textinput" value="'+elemento.emp_n+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput1">Almacén:</label><input type="text" name="textinput1" id="textinput1" value="'+elemento.alm_n+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput3">Composición:</label><input type="text" name="textinput3" id="textinput3" value="'+elemento.comp_n+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput4">Cantidad:</label><input type="text" name="textinput4" id="textinput4" value="'+elemento.cant+'" readonly /></div>';
			localStorage.estatus=elemento.estatus;
			localStorage.cant=elemento.cant;
			localStorage.comp=elemento.comp;
			localStorage.almd=elemento.alm;
			
		});
		html+='</ul></div>';
		if(localStorage.estatus=="A"){
			html+='<div  align="center" data-role="controlgroup" data-type="horizontal"><button data-theme="b" onClick="redireccionar(\'Despacho.html\')">Continuar Despacho</button><button data-theme="b" onClick="CerrarDesp()">Cerrar Despacho</button></div>';
		}
		$(html).appendTo('#CDesp');
		redireccionar("CDespachoE.html\#CDespachoE");
	});
}

function CerrarDesp(){
	mensaje="Error! Faltan artículos por despachar.";
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0001_consulta_composicion",{"TK":clave,"sec":localStorage.comp},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(j, elemento){
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_detalle_0003_consulta_despacho",{"TK":clave,"mov":localStorage.mov,"matcat":elemento.sec},function(resultado1){
				miRes1 = jQuery.parseJSON(resultado1);
				$.each(miRes1, function(i, elemento1){
					porcod = (parseInt(elemento.cant) * parseInt(localStorage.cant)) - parseInt(elemento1.despachados);
					if(porcod!=0){
						alert(mensaje);
						redireccionar("CDespachoE.html");
					}
				});
					jQuery.get(""+IP+":"+puerto+"/t_cor_inv_movimiento_0003_actualiza_estatus_despacho",{"TK":clave,"sec":localStorage.mov,"user":localStorage.nombre},function(resultado){
					redireccionar("CDespacho.html");
				});
			});
		});
	});
	
}

function Despacho(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0001_consulta_composicion",{"TK":clave,"sec":localStorage.comp},function(resultado){
		html='';
		html+='<div data-role="fieldcontain"><label for="CI">Código Inicial:</label><input type="number" name="CI" id="CI" min="0" maxlength="8"/></div>';
		html+='<div data-role="fieldcontain"><label for="CF">Código Final:</label><input type="number" name="CF" id="CF" min="0" maxlength="8"/></div>';
		html+='<button data-theme="b" onClick="RegCod()">Registrar</button> ';
		matcatsec= [];
		matcatxcod="[";
		matcat='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Componentes</h3></li>';
		miRes = jQuery.parseJSON(resultado);
		nro = 0;
		$.each(miRes, function(j, elemento){
			nro++;
		});
		$.each(miRes, function(j, elemento){
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_detalle_0003_consulta_despacho",{"TK":clave,"mov":localStorage.mov,"matcat":elemento.sec},function(resultado1){
				miRes1 = jQuery.parseJSON(resultado1);
				$.each(miRes1, function(i, elemento1){
					porcod = (parseInt(elemento.cant) * parseInt(localStorage.cant)) - parseInt(elemento1.despachados);
					matcatsec.push(elemento.sec);
					matcatxcod+=",["+elemento.sec+","+porcod+"]";
					matcat+='<li><h3>Artículo:'+elemento.id+'</h3><p>Por Codificar: '+porcod+'</p></li>';
				});
				if (j == (nro-1)) {
					matcat+='</ul>';
					localStorage.matcats=matcatsec;
					localStorage.matcatxcod=matcatxcod.replace(",","")+"]";
					html+=matcat;
					$(html).appendTo('#Despachar');
					redireccionar('Despacho.html\#Despacho');
				}
			});
		});
	});
}
/*function Despacho(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0001_consulta_composicion",{"TK":clave,"sec":localStorage.comp},function(resultado){
		html='';
		html+='<div data-role="fieldcontain"><label for="CI">Código Inicial:</label><input type="number" name="CI" id="CI" min="0"/></div>';
		html+='<div data-role="fieldcontain"><label for="CF">Código Final:</label><input type="number" name="CF" id="CF" min="0"/></div>';
		html+='<button data-theme="b" onClick="RegCod()">Registrar</button> ';
		matcatsec= [];
		matcat='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Componentes</h3></li>';
		miRes = jQuery.parseJSON(resultado);
		nro = 0;
		$.each(miRes, function(j, elemento){
			nro++;
		});
		$.each(miRes, function(j, elemento){
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_detalle_0003_consulta_despacho",{"TK":clave,"mov":localStorage.mov,"matcat":elemento.sec},function(resultado1){
				miRes1 = jQuery.parseJSON(resultado1);
				$.each(miRes1, function(i, elemento1){
					porcod = (parseInt(elemento.cant) * parseInt(localStorage.cant)) - parseInt(elemento1.despachados);
					matcatsec.push(elemento.sec);
					matcat+='<li><h3>Artículo:'+elemento.id+'</h3><p>Por Codificar: '+porcod+'</p></li>';
				});
				if (j == (nro-1)) {
					matcat+='</ul>';
					localStorage.matcats=matcatsec;
					html+=matcat;
					$(html).appendTo('#Despachar');
					redireccionar('Despacho.html\#Despacho');
				}
			});
		});
	});
}*/

function isValueInArray(arr, val) {
	inArray = false;
	for (i = 0; i < arr.length; i++)
		if (val == arr[i])
			inArray = true;
	return inArray;
}

function RegCod(){
	error=false;
	mensaje="Error! Los siguientes codigos son invalidos: ";
	CI=document.getElementById("CI").value;
	CF=document.getElementById("CF").value;
	if(CF==""){CF = parseInt(document.getElementById("CI").value);}
	if(CI==""){alert("Error! El campo Código Inicial es obligatorio.");return}
	CI = parseInt(document.getElementById("CI").value);
	/* Se realizan las siguientes validaciones:
	 *		1) El articulo se encuentra en el almacen que realizo la operacion
	 *		2) El articulo posee un matcat que conforme la composicion a despachar
	 *		3) El articulo se encuentra en estatus 138 (En Almacen)
	 */
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0002_consulta_validacion_despacho",{"TK":clave,"CI":CI,"CF":CF,"alm":localStorage.almd,"comp":localStorage.comp},function(resultado){
		nro=0;
		var cod = new Array();
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			nro++;
			cod[nro] = elemento.sec;
		});
		nroI = CF - CI + 1;
		if (nro != nroI) {
			for (i=CI; i <= CF; i++) {
				if (jQuery.inArray(i, cod) == -1)	{
					mensaje+=i +', ';
				}
				CI++;
			}
			alert (mensaje);
			return
		}else{
			jQuery.get("http://"+IP+":"+puerto+"/Despacho",{"TK":clave,"mov":localStorage.mov,"CI":CI,"CF":CF,"user":localStorage.nombre,"xcod":localStorage.matcatxcod},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				show=false;
				mensaje1= "Los siguientes artículos no fueron tomados en cuenta: "
				$.each(miRes2, function(i, elemento2){
					show=true;
					mensaje1+=elemento2.sec+",";
				});
				if(show){alert(mensaje1);}
				redireccionar("Despacho.html");
			});
		}
	});
}
/*function RegCod(){
	error=false;
	mensaje="Los siguientes codigos son invalidos: ";
	CI=document.getElementById("CI").value;
	CF=document.getElementById("CF").value;
	if(CF==""){CF = parseInt(document.getElementById("CI").value);}
	if(CI==""){alert("El campo Código Inicial es obligatorio.");return}
	CI = parseInt(document.getElementById("CI").value);
	/* Se realizan las siguientes validaciones:
	 *		1) El articulo se encuentra en el almacen que realizo la operacion
	 *		2) El articulo posee un matcat que conforme la composicion a despachar
	 *		3) El articulo se encuentra en estatus 138 (En Almacen)
	 */
	/*jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0002_consulta_validacion_despacho",{"TK":clave,"CI":CI,"CF":CF,"alm":localStorage.almd,"comp":localStorage.comp},function(resultado){
		nro=0;
		var cod = new Array();
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			nro++;
			cod[nro] = elemento.sec;
		});
		nroI = CF - CI + 1;
		if (nro != nroI) {
			for (i=CI; i <= CF; i++) {
				if (jQuery.inArray(i, cod) == -1)	{
					mensaje+=i +', ';
				}
				CI++;
			}
			alert (mensaje);
			return
		}else{
			jQuery.get("http://"+IP+":"+puerto+"/Despacho",{"mov":localStorage.mov,"CI":CI,"CF":CF,"user":localStorage.nombre},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				$.each(miRes2, function(i, elemento2){
				});
				redireccionar("Despacho.html");
			});
		}
	});
}*/

/********************************************************************************************************************************************/
/***                                                          PANTALLAS COMPOSICION                                                       ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de las comosiciones registradas en el sistema
 * 2012-09-17 MT
 */
function ConsultaComp(){
	//Servicio que busca las composiciones
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0002_consulta_composiciones",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado); 
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod">';
		html2 = '';
		nro = 0;
		$.each(miRes, function(i, elemento){
			html2+='<li><a onClick="ConsultarComposicion('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
			nro++;
		});
		html+='<li data-role="list-divider" role="heading"><h3>Composiciones: '+nro+'</h3></li>'+html2+'</ul>';
		$(html).appendTo("#ConsultaComp");
		redireccionar("ConsultaComp.html\#consulta_comp");
	});
}

/* Iserta en base de datos, los datos de la nueva composición
 * 2012-09-17 MT
 */
function creaComp() {
	id=document.getElementById("comp_id").value;
	desc=document.getElementById("comp_desc").value;
	act = document.getElementById("activo").value;
	if (id != "") {	
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0003_inserta_composicion",{"TK":clave,"id":id,"desc":desc,"activo":act,"user":localStorage.nombre},function(resultado){
			redireccionar("ConsultaComp.html");
		});
	} else {
		alert('Error! El campo Composición es obligatorio.');
		return
	}
}

/* Carga los datos de una composición especifica
 * 2012-09-17 MT
 */
function ConsultarComposicion(comp) {
	localStorage.comp = comp;
	redireccionar('CComposicion.html');	
}

/* Carga los datos de una composicion especifica
 * 2012-09-17 MT
 */
function DetallesComp(){
	html = "";
	// Se obtine la informacion de la composicion
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0004_consulta_composicion",{"TK":clave,"sec":localStorage.comp},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			html += '<label for="comp_id">Composición:</label><input type="text" name="comp_id" id="comp_id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="comp_desc">Descripción:</label><textarea cols="40" rows="8" name="comp_desc" id="comp_desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		// Se obtiene la información de los matcat que la componen
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0001_consulta_composicion_detalles",{"TK":clave,"sec":localStorage.comp},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro = 0;
			items = 0;
			html2 = '';
			$.each(miRes, function(i, elemento){
				html2 += '<li><a onClick="ConsultarMatcat('+elemento.matcat+')"><h3>'+elemento.id+'</h3><p>Cantidad: '+elemento.cant+'</p></a><a onClick="EliminarMatcat('+elemento.matcat+')">Predet.</a></li>'
				nro++;
				items+=parseInt(elemento.cant); 
			});
			// Se agregan los botones
			html += '<div align="center" data-role="controlgroup" data-type="horizontal">';
			html += "<button data-inline='true' data-theme='b' onClick='ActualizarComp()'>Actualizar</button>";
			html += '<button data-inline="true" data-theme="b" onClick="AgregarMatcat()">Agregar Matrerial</button>';
			// Se agrega la lista de materiales de la composicion
			html += '</div><ul data-role="listview" data-inset="true" data-split-icon="delete" id="ConsultaCod">';
			html += '<li data-role="list-divider" role="heading">Nro. de Materiales: '+nro+'; Nro. de Artículos: '+items+'</li>'+html2+'</ul>';
			$(html).appendTo('#Datos');
			redireccionar('CComposicion.html\#Comp');
		});
	});
}

/* Actualiza los datos de una Composicion
 * 2012-09-17 MT
 */
function ActualizarComp() {
	id=document.getElementById("comp_id").value;
	desc=document.getElementById("comp_desc").value;
	act = document.getElementById("activo").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0005_actualiza_composicion",{"TK":clave,"id":id,"desc":desc,"act":localStorage.activo,"sec":localStorage.comp},function(resultado){
		redireccionar('CComposicion.html');
	});
}

function AgregarMatcat() {
	redireccionar('CComposicionDet.html');
}
/* Carga los selects de la pantalla de agregar matcat a composicion
 * 2012-09-17 MT
 */
function AgregarMaterial() {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_matcat",{"TK":clave},function(resultado){
		html = '<label for="matcat" class="select">Material:</label><select name="matcat" id="matcat" data-theme="b"><option value="">Seleccione</option>';
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			html += '<option value="'+elemento.sec+'">'+elemento.id+'</option>';
		});
		html += '</select><label for="cant">Cantidad:</label><input type="number" name="cant" id="cant" value="0" min="0"/>';
		html += '<a onClick="InsertarMatcat()" data-role="button" data-theme="b">Agregar</a>';
		$(html).appendTo('#Datos');
		redireccionar('CComposicionDet.html\#Comp');
	});
}

/* Inserta el matererial en la composicion
 * 2012-09-17 MT
 */
function InsertarMatcat() {
	select=document.getElementById("matcat").options;
	index=document.getElementById("matcat").selectedIndex;
	matcat = select[index].value;
	cant = document.getElementById("cant").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0002_inserta_material",{"TK":clave,"comp":localStorage.comp,"matcat":matcat,"cant":cant,"user":localStorage.nombre},function(resultado){
		redireccionar('CComposicion.html');
	});
}

/* Elimina un matererial en la composicion
 * 2012-09-18 MT
 */
function EliminarMatcat(matcatsec){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0003_elimina_material",{"TK":clave,"comp":localStorage.comp,"matcat":matcatsec},function(resultado){
		redireccionar('CComposicion.html');
	});
}

/* Guarda los datos del material a modificar
 * 2012-09-18 MT
 */
function ConsultarMatcat(matcatsec){
	localStorage.matcatsec = matcatsec;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0004_consulta_composicion_detalles",{"TK":clave,"sec":localStorage.comp,"matcat":matcatsec},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			localStorage.matcatid = elemento.id;
			localStorage.matcatcant = elemento.cant;
		});
		redireccionar('ModificarCantidad.html');
	});
}

/* Carga los datos del material a modificar
 * 2012-09-18 MT
 */
function DetallesModificar() {
	html = '<label for="cant">Material:</label><input type="text" name="ct" id="ct" value="'+localStorage.matcatid+'" readonly/>';
	html += '<label for="cant">Cantidad:</label><input type="number" name="cant" id="cant" value="'+localStorage.matcatcant+'" min="1"/>';
	html += '<a onClick="ModMatcat()" data-role="button" data-theme="b">Actualizar</a>';
	$(html).appendTo('#Datos');
	redireccionar('ModificarCantidad.html\#Comp');
}

/* Modifica los datos del material a modificar
 * 2012-09-18 MT
 */
function ModMatcat() {
	cant = document.getElementById("cant").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_detalle_0005_actualiza_cantidad",{"TK":clave,"cant":cant,"sec":localStorage.comp,"matcat":localStorage.matcatsec},function(resultado){
		redireccionar('CComposicion.html');
	});
}

/********************************************************************************************************************************************/
/***                                                          PANTALLAS MATERIA PRIMA                                                     ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de las materias primas
 * 2012-09-19 MT
 */
function ConsultaMP(){
	//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0002_consulta_materia_prima",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesMPsec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Materias Primas: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaMP.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de una materia prima
 * 2012-09-19 MT
 */
function DetallesMPsec(sec){
	localStorage.mp = sec;
	redireccionar("DetalleMP.html");
}

/* Obtiene los detalles de una materia prima
 * 2012-09-19 MT
 */
function DetallesMP(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0003_consulta_materia_prima_detalle",{"TK":clave,"mp":localStorage.mp},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Materia Prima:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarMP('+localStorage.mp+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleMP.html\#CResponsable");
	});
}

/* Actualiza los datos de una materia prima
 * 2012-09-19 MT
 */
function ActualizarMP(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0004_actualiza_materia_prima",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.mp},function(resultado){
		redireccionar("DetalleMP.html");
	});
}

/* Carga el formulario de inserción de materia prima
 * 2012-09-19 MT
 */
function NewMP(){
	html = '<label for="id">Materia Prima:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarMP()" data-role="button" data-theme="b">Crear Materia Prima</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewMP.html\#CResponsable");	
}

/* Inserción de materia prima
 * 2012-09-19 MT
 */
function InsertarMP() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0005_inserta_materia_prima",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaMP.html");
	});
}

/********************************************************************************************************************************************/
/***                                                              PANTALLAS COLOR   	                                                  ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de colores
 * 2012-09-19 MT
 */
function ConsultaColor(){
	//Servicio que busca los datos de las recepciones que cumplen el rango de fechas
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0002_consulta_colores",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesColorsec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Colores: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaColor.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un color
 * 2012-09-19 MT
 */
function DetallesColorsec(sec){
	localStorage.col = sec;
	redireccionar("DetalleColor.html");
}


/* Obtiene los detalles de un color
 * 2012-09-19 MT
 */
function DetallesColor(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0003_consulta_color_detalle",{"TK":clave,"mp":localStorage.col},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Color:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarColor('+localStorage.col+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleColor.html\#CResponsable");
	});
}

/* Actualiza los datos de un Color
 * 2012-09-19 MT
 */
function ActualizarColor(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0004_actualiza_color",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.col},function(resultado){
		redireccionar("DetalleColor.html");
	});
}

/* Carga el formulario de inserción de Color
 * 2012-09-19 MT
 */
function NewColor(){
	html = '<label for="id">Color:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarColor()" data-role="button" data-theme="b">Crear Color</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewColor.html\#CResponsable");	
}

/* Inserción de Color
 * 2012-09-19 MT
 */
function InsertarColor() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0005_inserta_color",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaColor.html");
	});
}


/********************************************************************************************************************************************/
/***                                                              PANTALLAS GRUPO   	                                                  ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Grupos
 * 2012-09-19 MT
 */
function ConsultaGrupo(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0002_consulta_grupo",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesGruposec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Grupos: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaGrupo.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un Grupo
 * 2012-09-19 MT
 */
function DetallesGruposec(sec){
	localStorage.grupo = sec;
	redireccionar("DetalleGrupo.html");
}

/* Obtiene los detalles de un Grupo
 * 2012-09-19 MT
 */
function DetallesGrupo(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0003_consulta_grupo_detalle",{"TK":clave,"mp":localStorage.grupo},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Grupo:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarGrupo('+localStorage.grupo+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleGrupo.html\#CResponsable");
	});
}

/* Actualiza los datos de un Grupo
 * 2012-09-19 MT
 */
function ActualizarGrupo(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0004_actualiza_grupo",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.grupo},function(resultado){
		redireccionar("DetalleGrupo.html");
	});
}

/* Carga el formulario de inserción de Grupo
 * 2012-09-19 MT
 */
function NewGrupo(){
	html = '<label for="id">Grupo:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarGrupo()" data-role="button" data-theme="b">Crear Grupo</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewGrupo.html\#CResponsable");	
}

/* Inserción de Grupo
 * 2012-09-19 MT
 */
function InsertarGrupo() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0005_inserta_grupo",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaGrupo.html");
	});
}

/********************************************************************************************************************************************/
/***                                                           PANTALLAS SUBGRUPO   	                                                  ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Subgrupos
 * 2012-09-19 MT
 */
function ConsultaSGrupo(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0002_consulta_subgrupo",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesSGruposec('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Grupo: '+elemento.n1+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Subgrupos: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaSGrupo.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un Subrupo
 * 2012-09-19 MT
 */
function DetallesSGruposec(sec){
	localStorage.sgrupo = sec;
	redireccionar("DetalleSGrupo.html");
}

/* Obtiene los detalles de un Subrupo
 * 2012-09-19 MT
 */
function DetallesSGrupo(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0003_consulta_subgrupo_detalle",{"TK":clave,"mp":localStorage.sgrupo},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		html2 ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Subgrupo:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			localStorage.n1 = elemento.n1;
			if (elemento.activo == 'S') {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
			miRes2 = jQuery.parseJSON(resultado2);
			html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo"><option value="">Seleccione</option>';
			$.each(miRes2, function(i, elemento2){
				if (localStorage.n1 == elemento2.sec) {
					html3 +='<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
				} else {
					html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
				}				
			});
			html3+='</select></div>';
			html += html3 + ' ' + html2 + '<a onClick="ActualizarSGrupo('+localStorage.sgrupo+')" data-role="button" data-theme="b">Actualizar</a>';
			$(html).appendTo('#Resp');
			redireccionar("DetalleSGrupo.html\#CResponsable");
		});
	});
}

/* Actualiza los datos de un Subgrupo
 * 2012-09-20 MT
 */
function ActualizarSGrupo(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0004_actualiza_subgrupo",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo,"sec":localStorage.sgrupo},function(resultado){
		redireccionar("DetalleSGrupo.html");
	});
}

/* Carga el formulario de inserción de Subgrupo
 * 2012-09-20 MT
 */
function NewSGrupo(){
	html = '<label for="id">Subgrupo:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
		miRes2 = jQuery.parseJSON(resultado2);
		html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo"><option value="">Seleccione</option>';
		$.each(miRes2, function(i, elemento2){
			html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
		});
		html3+='</select></div><label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
		html += html3 + '<a onClick="InsertarSGrupo()" data-role="button" data-theme="b">Crear Subgrupo</a>';
		$(html).appendTo('#Resp');
		redireccionar("NewSGrupo.html\#CResponsable");
	});
}

/* Inserción de Grupo
 * 2012-09-19 MT
 */
function InsertarSGrupo() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0005_inserta_subgrupo",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo},function(resultado){
		redireccionar("ConsultaSGrupo.html");
	});
}

/********************************************************************************************************************************************/
/***                                                           PANTALLAS CLASE      	                                                  ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Clases
 * 2012-09-20 MT
 */
function ConsultaClase(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0002_consulta_clase",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesClasesec('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Grupo: '+elemento.n1+'; Subgrupo: '+elemento.n2+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Clases: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaClase.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de una Clase
 * 2012-09-20 MT
 */
function DetallesClasesec(sec){
	localStorage.clase = sec;
	redireccionar("DetalleClase.html");
}

/* Obtiene los detalles de una Clase
 * 2012-09-20 MT
 */
function DetallesClase(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0003_consulta_clase_detalle",{"TK":clave,"mp":localStorage.clase},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		html2 ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Clase:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"  style=" text-transform:uppercase"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)" style=" text-transform:uppercase">'+elemento.desc+'</textarea>';
			localStorage.n1 = elemento.n1;
			localStorage.n2 = elemento.n2;
			if (elemento.activo == 'S') {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
			miRes2 = jQuery.parseJSON(resultado2);
			html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo" onChange="actualizarSubGrupo()"><option value="">Seleccione</option>';
			$.each(miRes2, function(i, elemento2){
				if (localStorage.n1 == elemento2.sec) {
					html3 +='<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
				} else {
					html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
				}				
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0006_consulta_subgrupo_activos",{"TK":clave,"n1":localStorage.n1},function(resultado3){
				miRes3 = jQuery.parseJSON(resultado3);
				html4 = '<div data-role="fieldcontain"><label for="sgrupo">Subgrupo:</label><select data-theme="b" name="sgrupo" id="sgrupo"><option value="">Seleccione</option>';
				$.each(miRes3, function(i, elemento3){
					if (localStorage.n2 == elemento3.sec) {
						html4 +='<option value="'+elemento3.sec+'" selected>'+elemento3.id+'</option>';
					} else {
						html4 +='<option value="'+elemento3.sec+'">'+elemento3.id+'</option>';
					}				
				});
				html3 += '</select></div>';
				html4 += '</select></div>';
				html += html3 + html4 + html2 + '<a onClick="ActualizarClase('+localStorage.clase+')" data-role="button" data-theme="b">Actualizar</a>';
				$(html).appendTo('#Resp');
				redireccionar("DetalleClase.html\#CResponsable");
			});
		});
	});
}

/* Actualiza los datos de un Clase
 * 2012-09-20 MT
 */
function ActualizarClase(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	select=document.getElementById("sgrupo").options;
	index=document.getElementById("sgrupo").selectedIndex;
	sgrupo = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0004_actualiza_clase",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo,"n2":sgrupo,"sec":localStorage.clase},function(resultado){
		redireccionar("DetalleClase.html");
	});
}

/* Actualiza los datos sel select de SubGrupo al cambiar el valor del select de Grupos
 * 2012-09-20 MT
 */
function actualizarSubGrupo() {
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0006_consulta_subgrupo_activos",{"TK":clave,"n1":grupo},function(resultado){
		miRes2 = jQuery.parseJSON(resultado);
		acumulado2 = "<option value=''>Seleccione</option>";		
		$.each(miRes2, function(i, elemento){
			acumulado2 += "<option id=" + elemento.sec + " value='" + elemento.sec + "'>" + elemento.id + "</option>";
		});
		$("#sgrupo").empty();
		$("#sgrupo").html(acumulado2).selectmenu('refresh', true);
	});
}

/* Carga el formulario de inserción de Clase
 * 2012-09-20 MT
 */
function NewClase(){
	html = '<label for="id">Clase:</label><input type="text" name="id" id="id" value="" maxlength="32"  style=" text-transform:uppercase"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)" style=" text-transform:uppercase"></textarea>';
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
		miRes2 = jQuery.parseJSON(resultado2);
		html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo" onChange="actualizarSubGrupo()"><option value="">Seleccione</option>';
		$.each(miRes2, function(i, elemento2){
			html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
		});
		html3+='</select></div>';
		html4 = '<div data-role="fieldcontain"><label for="sgrupo">Subgrupo:</label><select data-theme="b" name="sgrupo" id="sgrupo"><option value="">Seleccione</option></select></div><label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
		html += html3 + html4 + '<a onClick="InsertarClase()" data-role="button" data-theme="b">Crear Clase</a>';
		$(html).appendTo('#Resp');
		redireccionar("NewClase.html\#CResponsable");
	});
}

/* Inserción de Clase
 * 2012-09-19 MT
 */
function InsertarClase() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	select=document.getElementById("sgrupo").options;
	index=document.getElementById("sgrupo").selectedIndex;
	sgrupo = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0005_inserta_clase",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo,"n2":sgrupo},function(resultado){
		redireccionar("ConsultaClase.html");
	});
}

/********************************************************************************************************************************************/
/***                                                           PANTALLAS MATERIAL      	                                                  ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Material
 * 2012-09-20 MT
 */
function ConsultaMaterial(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0002_consulta_material",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesMaterialsec('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>Grupo: '+elemento.n1+'; Subgrupo: '+elemento.n2+'; Clase: '+elemento.n3+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Materiales: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaMaterial.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un Material
 * 2012-09-20 MT
 */
function DetallesMaterialsec(sec){
	localStorage.mat = sec;
	redireccionar("DetalleMaterial.html");
}

/* Obtiene los detalles de un Material
 * 2012-09-20 MT
 */
function DetallesMaterial(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0003_consulta_material_detalle",{"TK":clave,"mp":localStorage.mat},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		html2 ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Material:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"  style=" text-transform:uppercase"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)" style=" text-transform:uppercase">'+elemento.desc+'</textarea>';
			localStorage.n1 = elemento.n1;
			localStorage.n2 = elemento.n2;
			localStorage.n3 = elemento.n3;
			if (elemento.activo == 'S') {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
			miRes2 = jQuery.parseJSON(resultado2);
			html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo" onChange="actualizarSubGrupo()"><option value="">Seleccione</option>';
			$.each(miRes2, function(i, elemento2){
				if (localStorage.n1 == elemento2.sec) {
					html3 +='<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
				} else {
					html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
				}				
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv2_0006_consulta_subgrupo_activos",{"TK":clave,"n1":localStorage.n1},function(resultado3){
				miRes3 = jQuery.parseJSON(resultado3);
				html4 = '<div data-role="fieldcontain"><label for="sgrupo">Subgrupo:</label><select data-theme="b" name="sgrupo" id="sgrupo" onChange="actualizarClase()"><option value="">Seleccione</option>';
				$.each(miRes3, function(i, elemento3){
					if (localStorage.n2 == elemento3.sec) {
						html4 +='<option value="'+elemento3.sec+'" selected>'+elemento3.id+'</option>';
					} else {
						html4 +='<option value="'+elemento3.sec+'">'+elemento3.id+'</option>';
					}				
				});
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0006_consulta_clases_activos",{"TK":clave,"n1":localStorage.n1,"n2":localStorage.n2},function(resultado4){
					miRes4 = jQuery.parseJSON(resultado4);
					html5 = '<div data-role="fieldcontain"><label for="clase">Clase:</label><select data-theme="b" name="clase" id="clase"><option value="">Seleccione</option>';
					$.each(miRes4, function(i, elemento4){
						if (localStorage.n3 == elemento4.sec) {
							html5 +='<option value="'+elemento4.sec+'" selected>'+elemento4.id+'</option>';
						} else {
							html5 +='<option value="'+elemento4.sec+'">'+elemento4.id+'</option>';
						}				
					});
					html3 += '</select></div>';
					html4 += '</select></div>';
					html5 += '</select></div>';
					html += html3 + html4 + html5 + html2 + '<a onClick="ActualizarMaterial('+localStorage.mat+')" data-role="button" data-theme="b">Actualizar</a>';
					$(html).appendTo('#Resp');
					redireccionar("DetalleMaterial.html\#CResponsable");
				});
			});
		});
	});
}

/* Actualiza los datos sel select de Clases al cambiar el valor del select de Subgrupos
 * 2012-09-20 MT
 */
function actualizarClase() {
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	select=document.getElementById("sgrupo").options;
	index=document.getElementById("sgrupo").selectedIndex;
	sgrupo = select[index].value;
	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv3_0006_consulta_clases_activos",{"TK":clave,"n1":grupo,"n2":sgrupo},function(resultado){
		miRes2 = jQuery.parseJSON(resultado);
		acumulado2 = "<option value=''>Seleccione</option>";		
		$.each(miRes2, function(i, elemento){
			acumulado2 += "<option id=" + elemento.sec + " value='" + elemento.sec + "'>" + elemento.id + "</option>";
		});
		$("#clase").empty();
		$("#clase").html(acumulado2).selectmenu('refresh', true);
	});
}

/* Actualiza los datos de un Material
 * 2012-09-20 MT
 */
function ActualizarMaterial(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	select=document.getElementById("sgrupo").options;
	index=document.getElementById("sgrupo").selectedIndex;
	sgrupo = select[index].value;
	select=document.getElementById("clase").options;
	index=document.getElementById("clase").selectedIndex;
	clase = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0004_actualiza_material",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo,"n2":sgrupo,"n3":clase,"sec":localStorage.mat},function(resultado){
		redireccionar("DetalleMaterial.html");
	});
}

/* Carga el formulario de inserción de Material
 * 2012-09-20 MT
 */
function NewMaterial(){
	html = '<label for="id">Material:</label><input type="text" name="id" id="id" value="" maxlength="32"  style=" text-transform:uppercase"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)" style=" text-transform:uppercase"></textarea>';
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_niv1_0001_consulta_grupo_activos",{"TK":clave},function(resultado2){
		miRes2 = jQuery.parseJSON(resultado2);
		html3 = '<div data-role="fieldcontain"><label for="grupo">Grupo:</label><select data-theme="b" name="grupo" id="grupo" onChange="actualizarSubGrupo()"><option value="">Seleccione</option>';
		$.each(miRes2, function(i, elemento2){
			html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
		});
		html3+='</select></div>';
		html4 = '<div data-role="fieldcontain"><label for="sgrupo">Subgrupo:</label><select data-theme="b" name="sgrupo" id="sgrupo" onChange="actualizarClase()"><option value="">Seleccione</option></select></div>';
		html5 = '<div data-role="fieldcontain"><label for="clase">Clase:</label><select data-theme="b" name="clase" id="clase"><option value="">Seleccione</option></select></div><label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
		html += html3 + html4 + html5 + '<a onClick="InsertarMaterial()" data-role="button" data-theme="b">Crear Material</a>';
		$(html).appendTo('#Resp');
		redireccionar("NewMaterial.html\#CResponsable");
	});
}

/* Inserción de Material
 * 2012-09-19 MT
 */
function InsertarMaterial() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("grupo").options;
	index=document.getElementById("grupo").selectedIndex;
	grupo = select[index].value;
	select=document.getElementById("sgrupo").options;
	index=document.getElementById("sgrupo").selectedIndex;
	sgrupo = select[index].value;
	select=document.getElementById("clase").options;
	index=document.getElementById("clase").selectedIndex;
	clase = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0005_inserta_material",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"n1":grupo,"n2":sgrupo,"n3":clase},function(resultado){
		redireccionar("ConsultaMaterial.html");
	});
}

/********************************************************************************************************************************************/
/***                                                        PANTALLAS TIPO RESPONSABLE                                                    ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Tipos de Responsables
 * 2012-09-20 MT
 */
function ConsultaTR(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_responsable_0002_consulta_tipo_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesTRsec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Tipo Responsable: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaTR.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un Grupo
 * 2012-09-20 MT
 */
function DetallesTRsec(sec){
	localStorage.tr = sec;
	redireccionar("DetalleTR.html");
}

/* Obtiene los detalles de un Tipo Responsable
 * 2012-09-20 MT
 */
function DetallesTR(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_responsable_0003_consulta_tipo_responsable_detalle",{"TK":clave,"sec":localStorage.tr},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Tipo Responsable:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarTR('+localStorage.tr+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleTR.html\#CResponsable");
	});
}

/* Actualiza los datos de un Tipo Responsable
 * 2012-09-20 MT
 */
function ActualizarTR(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_responsable_0004_actualiza_tipo_responsable",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.tr},function(resultado){
		redireccionar("DetalleTR.html");
	});
}

/* Carga el formulario de inserción de Tipo Responsable
 * 2012-09-20 MT
 */
function NewTR(){
	html = '<label for="id">Tipo Responsable:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarTR()" data-role="button" data-theme="b">Crear Tipo Responsable</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewTR.html\#CResponsable");	
}

/* Inserción de Tipo Responsable
 * 2012-09-20 MT
 */
function InsertarTR() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_responsable_0005_inserta_tipo_responsable",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaTR.html");
	});
}

/********************************************************************************************************************************************/
/***                                         		               PANTALLAS EMPINST                                                     ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Empresa / Institucion
 * 2012-09-20 MT
 */
function ConsultaEmpisnt(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_0002_consulta_empinst",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			comp = elemento.tipo;
			if (elemento.cliente == 'S') {
				comp += '; Cliente'
			}
			if (elemento.prov == 'S') {
				comp += '; Proveedor'
			}
			if (elemento.benef == 'S') {
				comp += '; Beneficiario.'
			}
			html+='<li><a onClick="DetallesEmpinstsec('+elemento.sec+')"><h3>'+elemento.id+'</h3><p>'+comp+'</p></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Empresa / Institución: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaEmpinst.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de una Empinst
 * 2012-09-20 MT
 */
function DetallesEmpinstsec(sec){
	localStorage.empinst = sec;
	redireccionar("DetalleEmpinst.html");
}

/* Obtiene los detalles de una Empinst
 * 2012-09-20 MT
 */
function DetallesEmpinst(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_0003_consulta_empinst_detalle",{"TK":clave,"sec":localStorage.empinst},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Empinst:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Nombre:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.nombre+'</textarea>';
			html += '<fieldset data-role="controlgroup">';
			rif = '';
			if (elemento.cliente == 'S') {
				html += '<label for="cliente">Cliente</label><input type="checkbox" onChange="actaccliente()" name="cliente" id="cliente" value="S" checked/>';
			} else {
				html += '<label for="cliente">Cliente</label><input type="checkbox" onChange="actaccliente()" name="cliente" id="cliente" value="S"/>';	
			}
			if (elemento.prov == 'S') {
				html += '<label for="prov">Proveedor</label><input type="checkbox" onChange="actaccliente()" name="prov" id="prov" value="S" checked/>';
			} else {
				html += '<label for="prov">Proveedor</label><input type="checkbox" onChange="actaccliente()" name="prov" id="prov" value="S"/>';	
			}
			if (elemento.benef == 'S') {
				html += '<label for="benef">Beneficiario</label><input type="checkbox" onChange="actaccliente()" name="benef" id="benef" value="S" checked/></fieldset>';
			} else {
				html += '<label for="benef">Beneficiario</label><input type="checkbox" onChange="actaccliente()" name="benef" id="benef" value="S"/></fieldset>';	
			}
			if (elemento.rif == 'None') {
				rif = '';
			} else {
				rif = elemento.rif;
			}
			html2 = '<label for="rif">RIF:</label><input type="text" name="rif" id="rif" value="'+rif+'" maxlength="20"/>';
			if (elemento.activo == 'S') {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html2 += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
			localStorage.tip = elemento.tipo;
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0002_consulta_tipo_empisnt_activo",{"TK":clave},function(resultado2){
			miRes2 = jQuery.parseJSON(resultado2);
			html3 ='<div data-role="fieldcontain"><label for="tipo">Tipo:</label><select data-theme="b" name="tipo" id="tipo"><option value="">Seleccione</option>';
			$.each(miRes2, function(i, elemento2){
				if (localStorage.tip == elemento2.sec) {
					html3 +='<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
				} else {
					html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
				}
			});
			html3 += '</select></div>';
			html2 += '<a onClick="ActualizarEmpinst('+localStorage.empinst+')" data-role="button" data-theme="b">Actualizar</a>';
			html += html3 + html2;
			$(html).appendTo('#Resp');
			redireccionar("DetalleEmpinst.html\#CResponsable");
		});
	});
}

/* Actualiza los datos de un Tipo Responsable
 * 2012-09-20 MT
 */
function ActualizarEmpinst(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	tipo = select[index].value;
	rif = document.getElementById("rif").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_0004_actualiza_empisnt",{"TK":clave,"user":localStorage.nombre,"id":id,"nombre":desc,"activo":localStorage.activo,"sec":localStorage.empinst,"cliente":localStorage.c,"prov":localStorage.p,"benef":localStorage.b,"tipo":tipo,"rif":rif},function(resultado){
		redireccionar("DetalleEmpinst.html");
	});
}

/* Carga el formulario de inserción de Empinst
 * 2012-09-21 MT
 */
function NewEmpinst(){
	html='<label for="id">Empinst:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html+='<label for="desc">Nombre:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html+='<fieldset data-role="controlgroup"><label for="cliente">Cliente</label><input type="checkbox" onChange="actaccliente()" name="cliente" id="cliente" value="S"/>';
	html+='<label for="prov">Proveedor</label><input type="checkbox" onChange="actaccliente()" name="prov" id="prov" value="S"/>';	
	html+='<label for="benef">Beneficiario</label><input type="checkbox" onChange="actaccliente()" name="benef" id="benef" value="S"/></fieldset>';	
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0002_consulta_tipo_empisnt_activo",{"TK":clave},function(resultado2){
		miRes2 = jQuery.parseJSON(resultado2);
		html3 ='<div data-role="fieldcontain"><label for="tipo">Tipo:</label><select data-theme="b" name="tipo" id="tipo"><option value="">Seleccione</option>';
		$.each(miRes2, function(i, elemento2){
			html3 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
		});
		html3 += '</select></div>';
		html += html3 + '<label for="rif">RIF:</label><input type="text" name="rif" id="rif" value="" maxlength="20"/>';
		html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
		html+='<a onClick="InsertarEmpinst()" data-role="button" data-theme="b">Crear Empinst</a>';
		$(html).appendTo('#Resp');
			redireccionar("NewEmpinst.html\#CResponsable");
		});
}

/* Inserción de Empinst
 * 2012-09-21 MT
 */
function InsertarEmpinst() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	select=document.getElementById("tipo").options;
	index=document.getElementById("tipo").selectedIndex;
	tipo = select[index].value;
	rif = document.getElementById("rif").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_0005_inserta_empisnt",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"cliente":localStorage.c,"prov":localStorage.p,"tipo":tipo,"activo":localStorage.activo,"rif":rif,"benef":localStorage.b},function(resultado){
		redireccionar("ConsultaEmpinst.html");
	});
}
/********************************************************************************************************************************************/
/***                              		                          PANTALLAS EMPINST                            						      ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Tipo de Empresa / Institucion
 * 2012-09-20 MT
 */
function ConsultaTE(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0001_consulta_tipo_empisnt",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesTEsec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Tipo Empresa / Institución: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaTE.html\#CResponsable");
	});	
}

/* Carga el formulario de inserción de Tipo Empinst
 * 2012-09-20 MT
 */
function NewTE(){
	html = '<label for="id">Tipo Empinst:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarTE()" data-role="button" data-theme="b">Crear Tipo Empinst</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewTE.html\#CResponsable");	
}

/* Inserción de Tipo Empist
 * 2012-09-20 MT
 */
function InsertarTE() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0005_inserta_tipo_empisnt",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaTE.html");
	});
}

/* Almacena el el localStorage el sec de un Tipo Empinst
 * 2012-09-20 MT
 */
function DetallesTEsec(sec){
	localStorage.te = sec;
	redireccionar("DetalleTE.html");
}

/* Obtiene los detalles de un Tipo Empinst
 * 2012-09-20 MT
 */
function DetallesTE(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0003_consulta_tipo_empinst_detalle",{"TK":clave,"sec":localStorage.te},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Tipo Empinst:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarTE('+localStorage.te+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleTE.html\#CResponsable");
	});
}

/* Actualiza los datos de un Tipo Empinst
 * 2012-09-20 MT
 */
function ActualizarTE(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_empinst_tipo_0004_actualiza_tipo_empisnt",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.te},function(resultado){
		redireccionar("DetalleTE.html");
	});
}


/********************************************************************************************************************************************/
/***                              		                          PANTALLAS ARTICULOS                          						      ***/
/********************************************************************************************************************************************/
/* Obtiene la lista de Articulos
 * 2012-09-21 MT
 */
function CArticulos(){
	$('#cod').val(localStorage.cod);
	if (localStorage.cod != '' || localStorage.t != '') {
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0004_consulta_articulo",{"TK":clave,"codigo":localStorage.cod,"tipo":localStorage.t},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			$.each(miRes, function(i, elemento){
				nro+=1;
				comp = "";
				if (elemento.compuesto == 'S') {
					comp += 'Compuesto.'
				}
				html+='<li><a onClick="DetallesArticulosec('+elemento.sec+')"><h3>'+elemento.sec+': '+elemento.matcat+'</h3><p>'+elemento.estatus+'; '+elemento.almacen+'; '+comp+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Artículos no despachados: '+nro+'</h3></li>'+html+'</ul>';
			comp="";
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_material_activas",{"TK":clave},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				html2='<div data-role="fieldcontain"><label for="tipo">Material:</label><select data-theme="b" name="tipo" id="tipo" onChange="actt()"><option value="">Seleccione</option>';
				$.each(miRes2, function(i, elemento2){
					if (localStorage.t == elemento2.sec) {
						html2 +='<option value="'+elemento2.sec+'" selected>'+elemento2.id+'</option>';
					} else {
						html2 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
					}
				});
				html2+='</select></div>';
				$(html2).appendTo('#matcat');
				$(html).appendTo('#Codif');
				redireccionar("CArticulos.html\#CCodificacion");
			});
		});
	} else {
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0003_consulta_articulos_no_desp",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado);
			nro=0;
			html="";
			$.each(miRes, function(i, elemento){
				nro+=1;
				comp = "";
				if (elemento.compuesto == 'S') {
					comp += 'Compuesto.'
				}
				html+='<li><a onClick="DetallesArticulosec('+elemento.sec+')"><h3>'+elemento.sec+': '+elemento.matcat+'</h3><p>'+elemento.estatus+'; '+elemento.almacen+'; '+comp+'</p></a></li>';
			});
			html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Artículos no despachados: '+nro+'</h3></li>'+html+'</ul>';
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_material_activas",{"TK":clave},function(resultado2){
				miRes2 = jQuery.parseJSON(resultado2);
				html2='<div data-role="fieldcontain"><label for="tipo">Material:</label><select data-theme="b" name="tipo" id="tipo" onChange="actt()"><option value="">Seleccione</option>';
				$.each(miRes2, function(i, elemento2){
					html2 +='<option value="'+elemento2.sec+'">'+elemento2.id+'</option>';
				});
				html2+='</select></div>';
				$(html2).appendTo('#matcat');
				$(html).appendTo('#Codif');
				redireccionar("CArticulos.html\#CCodificacion");
			});
		});
	}	
}

/* Almacena el el localStorage el sec del Articulo a consultar
 * 2012-09-21 MT
 */
function DetallesArticulosec(sec){
	localStorage.art = sec;
	localStorage.compuesto = 'N';
	redireccionar("DetalleArt.html");
}

/* Obtiene los detalles de un Tipo Empinst
 * 2012-09-20 MT
 */
function DetallesArticulo(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0005_consulta_articulo_detalles",{"TK":clave,"sec":localStorage.art},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html =''; html2 ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="sec">Código:</label><input type="text" name="sec" id="sec" value="'+localStorage.art+'" readonly/>';
			html += '<label for="matcat">Material:</label><input type="text" name="matcat" id="matcat" value="'+elemento.matcat+'" readonly/>';
			html += '<label for="alm">Alamcén:</label><input type="text" name="alm" id="alm" value="'+elemento.almacen+'" readonly/>';
			html += '<label for="est">Estatus:</label><input type="text" name="est" id="est" value="'+elemento.estatus+'" readonly/>';
			html += '<label for="serial">Serial:</label><input type="text" name="serial" id="serial" value="'+elemento.serial+'"/>';
			// Se cargan los datos de Color
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_color_0001_consulta_colores_activos",{"TK":clave},function(resultado3){
					miRes3 = jQuery.parseJSON(resultado3);
					color='<div data-role="fieldcontain"><label for="color">Color:</label><select data-theme="b" name="color" id="color" onChange="ActColor()"><option value="">Seleccione</option>';
					$.each(miRes3, function(i, elemento3){
						if (elemento.color == elemento3.sec) {
							color+='<option value="'+elemento3.sec+'" selected>'+elemento3.id+'</option>';
						} else {
							color+='<option value="'+elemento3.sec+'">'+elemento3.id+'</option>';	
						}
					});
					// Se cargan los datos de Materia Prima
					jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_materia_prima_0001_consulta_materia_prima_activa",{"TK":clave},function(resultado4){
						miRes4 = jQuery.parseJSON(resultado4);
						materiap='<div data-role="fieldcontain"><label for="matp">Materia Prima:</label><select data-theme="b" name="matp" id="matp" onChange="ActMatP()"><option value="">Seleccione</option>';
						$.each(miRes4, function(i, elemento4){
							if (elemento.mat_prima == elemento4.sec) {
								materiap+='<option value="'+elemento4.sec+'" selected>'+elemento4.id+'</option>';
							} else {
								materiap+='<option value="'+elemento4.sec+'">'+elemento4.id+'</option>';	
							}
						});
						materiap+='</select></div>';
						color+='</select></div>';
						html+='</select></div> ' + color + ' ' + materiap + ' ' + html2;
						if (elemento.compuesto == 'S') {
							html += '<div align="center" data-role="controlgroup" data-type="horizontal">'; componentes = ''; nro_comp = 0;
							html += '<button data-inline="true" data-theme="b" onClick="ActualizarArt('+localStorage.art+')">Actualizar</button>';
							html += '<button data-inline="true" data-theme="b" onClick="redireccionar('+"'AgregarComponente.html'"+')">Agregar Componente</button></div>';
							// Se carga la lista de Componentes
							jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_componente_0001_consulta_articulo_componente",{"TK":clave,"sec":localStorage.art},function(resultado5){
								miRes5 = jQuery.parseJSON(resultado5);
								$.each(miRes5, function(i, elemento5){
									nro_comp++;
									componentes += '<li><h3>'+elemento5.sec+': '+elemento5.matcat+'</h3><p>'+elemento5.estatus+'; '+elemento.almacen+'</p></li>'
								});
								html +='<ul data-role="listview" data-inset="true"><li data-role="list-divider" role="heading" data-theme="b">Componentes: '+nro_comp+'</li>'
								html += componentes + '</ul>';
								$(html).appendTo('#Resp');
								redireccionar("DetalleArt.html\#CResponsable");
							});
						} else {
							html += '<a onClick="ActualizarArt('+localStorage.art+')" data-role="button" data-theme="b">Actualizar</a>';
							$(html).appendTo('#Resp');
							redireccionar("DetalleArt.html\#CResponsable");
						}
					});
				});
			if (elemento.compuesto == 'S') {
				html2 += '<label for="comp">Compuesto</label> <input type="checkbox" onChange="actcomp()" name="comp" id="comp" value="S" checked/>';
			} else {
				html2 += '<label for="comp">Compuesto</label> <input type="checkbox" onChange="actcomp()" name="comp" id="comp" value="S"/>';
			}
			html2 += '<label for="desc">Observaciones:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
		});
	});
}

/* Actualiza los datos de un Articulo
 * 2012-09-21 MT
 */
function ActualizarArt(sec){
	desc = document.getElementById("desc").value;
	serial = document.getElementById("serial").value;
	select=document.getElementById("color").options;
	index=document.getElementById("color").selectedIndex;
	color = select[index].value;
	select=document.getElementById("matp").options;
	index=document.getElementById("matp").selectedIndex;
	matp = select[index].value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0005_actualiza_articulo",{"TK":clave,"user":localStorage.nombre,"desc":desc,"serial":serial,"color":color,"matprima":matp,"comp":localStorage.compuesto,"sec":localStorage.art},function(resultado){
		redireccionar("DetalleArt.html");
	});
}

/* Agregar componente
 * 2012-09-21 MT
 */
function AgregarComponente(){
	comp = document.getElementById("comp").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0001_consulta_verificar_codigos",{"TK":clave,"CI":comp,"CF":comp},function(resultado){
		var error = '';
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			error+=elemento.sec+', ';
		});
		if (error != ''){
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_componente__0002_inserta_componente",{"TK":clave,"comp":localStorage.art,"compto":comp},function(resultado){
				redireccionar("DetalleArt.html");
			});	
		} else {
			alert('Error! El Código no se encuentra registrado.');	
			return
		}	
	});
}

/* Funcion que carga la pantalla de consulta de artículos disponibles.
 * 24/09/2012 MT
 */
function ArticulosDisp() {
	if (localStorage.x == "") {
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_material_activas",{"TK":clave},function(resultado){
			var html = '<label for="matcat1" class="select">Material:</label><select name="matcat1" id="matcat1" data-theme="b" onChange="ActMater()"><option value="">Seleccione</option>';
			miRes = jQuery.parseJSON(resultado);
			$.each(miRes, function(i, elemento){
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0002_consulta_almacen_activo",{"TK":clave},function(resultado1){
				var html2 = '<label for="alm" class="select">Almacén:</label><select name="alm" id="alm" data-theme="b" onChange="ActAlma()"><option value="">Seleccione</option>';
				miRes1 = jQuery.parseJSON(resultado1);
				$.each(miRes1, function(i, elemento1){
					html2+='<option value="'+elemento1.sec+'">'+elemento1.id+'</option>';
				});
				html2+='</select>';
				html+='</select>' + html2;
				$(html).appendTo("#matcat");
				redireccionar("ArticulosDisp.html\#CCodificacion");
			});
		});
	} else {
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_material_activas",{"TK":clave},function(resultado){
			var html = '<label for="matcat1" class="select">Material:</label><select name="matcat1" id="matcat1" data-theme="b" onChange="ActMater()"><option value="">Seleccione</option>';
			miRes = jQuery.parseJSON(resultado);
			$.each(miRes, function(i, elemento){
				if (localStorage.x == elemento.sec) {
					html+='<option value="'+elemento.sec+'" selected>'+elemento.id+'</option>';
				} else {
					html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';	
				}
			});
			jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0002_consulta_almacen_activo",{"TK":clave},function(resultado1){
				var html2 = '<label for="alm" class="select">Almacén:</label><select name="alm" id="alm" data-theme="b" onChange="ActAlma()"><option value="">Seleccione</option>';
				miRes1 = jQuery.parseJSON(resultado1);
				$.each(miRes1, function(i, elemento1){
					if (localStorage.y == elemento1.sec) {
						html2+='<option value="'+elemento1.sec+'" selected>'+elemento1.id+'</option>';
					} else {
						html2+='<option value="'+elemento1.sec+'">'+elemento1.id+'</option>';	
					}
				});
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_articulo_0007_consulta_articulos_disponibles",{"TK":clave,"matcat":localStorage.x,"alm":localStorage.y},function(resultado2){
					var html3 = ''; html4 = "";
					nro = 0;
					miRes2 = jQuery.parseJSON(resultado2);
					$.each(miRes2, function(i, elemento2){
						nro += elemento2.cant;
						html3 += '<li><h3>Almacén: '+elemento2.alm_id+'; Cantidad: '+elemento2.cant+'</a></li>';
					});
					if (localStorage.y != "") {
						html4+='<ul data-role="listview" data-inset="true"><li data-role="list-divider" role="heading" data-theme="b"><h3>Artículos Disponibles: '+nro+'</h3></li></ul>'
					} else {
						html4+='<ul data-role="listview" data-inset="true"><li data-role="list-divider" role="heading" data-theme="b"><h3>Artículos Disponibles: '+nro+'</h3></li>'
						html4+= html3 + '</ul>';
					}
					html2+='</select>';
					html+='</select>' + html2;
					$(html).appendTo("#matcat");
					$(html4).appendTo("#matcat_lista");
					redireccionar("ArticulosDisp.html\#CCodificacion");
				});
			});
		});
	}
}

function limpiarFiltro(arg) {
	redireccionarC(arg);
}

function LotesXCodif() {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_material_activas",{"TK":clave},function(resultado){
		var html = '<label for="matcat1" class="select">Material:</label><select name="matcat1" id="matcat1" data-theme="b" onChange="ActMater()"><option value="">Seleccione</option>';
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			if (localStorage.x == elemento.sec) {
				html+='<option value="'+elemento.sec+'" selected>'+elemento.id+'</option>';
			} else {
				html+='<option value="'+elemento.sec+'">'+elemento.id+'</option>';
			}
		});
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_lote_detalle_0008_consultar_lotes_por_codificar",{"TK":clave,"matcat":localStorage.x},function(resultado2){
			var html3 = ''; html4 = "";
			nro = 0;
			miRes2 = jQuery.parseJSON(resultado2);
			$.each(miRes2, function(i, elemento2){
				nro += elemento2.sum;
				html3 += '<li><h3>'+elemento2.matcat_id+'</h3><p>Alamacén: '+elemento2.alm_id+'; Cantidad: '+elemento2.sum+'.<p></a></li>';
			});
			html4+='<ul data-role="listview" data-inset="true"><li data-role="list-divider" role="heading" data-theme="b"><h3>Lotes Por Codificar: </h3></li>'
			html4+= html3 + '</ul>';
			html+='</select>';
			$(html).appendTo("#matcat");
			$(html4).appendTo("#matcat_lista");
			redireccionar("LotesCodif.html\#CCodificacion");
		});
	});
}
/***********************************************************************************************************************************/
// Modificada 25/09/2012 MT
//Función que genera la pantalla de consultar composición
function ConsultarComp(){
	//Servicio que busca las composiciones
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0001_consulta_composicion_activo",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado); 
		html='<div data-role="fieldcontain"><label for="comp">Composición:</label><select data-theme="b" name="comp" id="comp" data-theme = "b" ><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			if(localStorage.comp==elemento.sec){
				html+= '<option value="'+elemento.sec+'" selected> '+elemento.id+'</option>';			
			}else{
				html+= '<option value="'+elemento.sec+'"> '+elemento.id+'</option>';
			}		
		});
		html+='</select></div>';
		$(html).appendTo("#ConsultaComp");
		
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0001_consulta_almacen",{"TK":clave},function(resultado){
			miRes = jQuery.parseJSON(resultado); 
			html='<div data-role="fieldcontain"><label for="alm">Almacen:</label><select data-theme="b" name="alm" id="alm" data-theme = "b" ><option value="">Seleccione</option>';
			$.each(miRes, function(i, elemento){
				if(localStorage.alm==elemento.sec){
					html+= '<option value="'+elemento.sec+'" selected> '+elemento.id+'</option>';			
				}else{
					html+= '<option value="'+elemento.sec+'"> '+elemento.id+'</option>';
				}		
			});
			html+='</select></div>';
			$(html).appendTo("#ArtA");
			
			var nro = 0;
			if(localStorage.comp!=''){
				//Servicio que busca los datos de todas las composiciones 
				jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_composicion_0002_consulta_composicion",{"TK":clave,"comp":localStorage.comp,"alm":localStorage.alm},function(resultado){
					miRes = jQuery.parseJSON(resultado);
					html2 = '';
					$.each(miRes,function(i,elemento){
						if(i==0){
							nro=elemento.dis;
						}else{
							html2+='<li>'+ elemento.matcat +': '+elemento.res+'</li>';
						}
					});
					html='<div data-role="content" id="Art" aling="center"><ul data-role="listview" data-inset="true" id="Restan"><li data-role="list-divider" role="heading"><h3>Disponibles: '+nro+'; Restan: </h3></li>';
					html+=html2 + '</ul></div>';
					$(html).appendTo("#ConsultaCompL");
					redireccionar("ConsComposiciones.html\#consulta_comp");
				});
			}else{redireccionar("ConsComposiciones.html\#consulta_comp");}
		});
	});	
}

//Modificada 24/09/2012 Alvaro
function Guardar(){
	select = document.getElementById("comp").options;
	index = document.getElementById("comp").selectedIndex;
	selectalm = document.getElementById("alm").options;
	indexalm = document.getElementById("alm").selectedIndex;
	if(index!=0){
		localStorage.comp=select[index].value;
		localStorage.alm=selectalm[indexalm].value;
		redireccionar("ConsComposiciones.html");
	}else{
		alert("Debe seleccionar la Composición a consultar.");
	}
}

// Modificada 24/09/2012 Alvaro
//Función auxiliar que inicializa las variables necesarias para consultar una composición	
function PreConsultarComposicion(){
	localStorage.comp="";
	localStorage.alm="";
	/*localStorage.CArt="";
	localStorage.CxA="";*/
	redireccionar("ConsComposiciones.html");
}

//New 24/09/2012 Alvaro
//Función que genera la pantalla de consultar una recepción seleccionada
function ConsultaInventario(){

	//Servicio que busca los datos de la codificación seleccionada
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_0002_consulta_inventario",{"TK":clave,"sec":localStorage.mov},function(resultado){
		miRes = jQuery.parseJSON(resultado);	
		html = "";
		$.each(miRes, function(i, elemento){
		
			html+='<div data-role="fieldcontain"><label for="textinput2">Identificador:</label><input type="text" name="textinput2" id="textinput2" value="'+elemento.id+'" readonly  /> </div>';
			html+='<div data-role="fieldcontain"><label for="textarea">Descripcion:</label><textarea cols="40" rows="8" name="textarea" id="textarea" readonly>'+elemento.desc+'</textarea></div>';
			html+='<div data-role="fieldcontain"><label for="textinput7">Fecha:</label><input type="text" name="textinput7" id="textinput7" value="'+elemento.fecha+'" readonly /> </div>';
			html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><input type="text" name="textinput5" id="textinput5" value="'+elemento.resp_n+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput">Almacen:</label><input type="text" name="textinput" id="textinput" value="'+elemento.alm_n+'" readonly /></div>';
			localStorage.estatus=elemento.estatus;
		});
		html+='</ul></div>';
		if(localStorage.estatus=="A"){
			html+='<div align="center" data-role="controlgroup" data-type="horizontal"><button data-inline="true" data-theme="b" onClick="redireccionar(\'Inventario.html\')">Continuar Inventario</button><button data-inline="true" data-theme="b" onClick="CerrarInventario()">Cerrar Inventario</button></div>';
		}
		html2 = '';
		jQuery.get("http://"+IP+":"+puerto+"/ListaInventario",{"TK":clave,"sec":localStorage.mov},function(resultado1){
			miRes1= jQuery.parseJSON(resultado1);
			nro = 0; cant = 0;
			$.each(miRes1, function(i, elemento){
				nro++;
				cant += elemento.cant;
				html2+='<li><h3>'+elemento.id+'</h3><p>Cantidad: '+elemento.cant+'</p></li>';
			});
			html+='<div data-role="content" id="lista" aling="center"><ul data-role="listview" data-inset="true" id="Listazona"><li data-role="list-divider" role="heading"><h3>Matriales contabilizados: '+nro+'; Nro. de Artículos contabilizados: '+cant+'</h3></li>' + html2 + '</ul></div>';	
		
			$(html).appendTo('#Inventario');
			redireccionar("CInventario1E.html\#CInventario1E");
		});
	});
}

//New 24/09/2012 Alvaro
//Función que genera la pantalla de consultar un traslado seleccionado
function ConsultaTraslado(){

	//Servicio que busca los datos de la codificación seleccionada
	jQuery.get("http://"+IP+":"+puerto+"/ConsultaTraslado",{"TK":clave,"sec":localStorage.mov},function(resultado){
		miRes = jQuery.parseJSON(resultado);	
		html = "";
		$.each(miRes, function(i, elemento){
		
			html+='<div data-role="fieldcontain"><label for="textinput2">Identificador:</label><input type="text" name="textinput2" id="textinput2" value="'+elemento.id+'" readonly  /> </div>';
			html+='<div data-role="fieldcontain"><label for="textarea">Descripcion:</label><textarea cols="40" rows="8" name="textarea" id="textarea" readonly>'+elemento.desc+'</textarea></div>';
			html+='<div data-role="fieldcontain"><label for="textinput7">Fecha:</label><input type="text" name="textinput7" id="textinput7" value="'+elemento.fecha+'" readonly /> </div>';
			html+='<div data-role="fieldcontain"><label for="textinput5">Responsable:</label><input type="text" name="textinput5" id="textinput5" value="'+elemento.resp+'" readonly /></div>';
			html+='<div data-role="fieldcontain"><label for="textinput">Motivo:</label><input type="text" name="textinput" id="textinput" value="'+elemento.motivo+'" readonly /></div>';
			localStorage.estatus=elemento.estatus;
		});
		html+='</ul></div>';
		if(localStorage.estatus=="A"){
			html+='<button data-theme="b" onClick="redireccionar(\'Traslado.html\')">Continuar Traslado</button><button data-theme="b" onClick="CerrarIorT()">Cerrar Traslado</button>';
		}
		html+='<div data-role="content" id="lista" aling="center"><ul data-role="listview" id="Listazona"><li data-role="list-divider" role="heading"><h3></h3></li>';

		jQuery.get("http://"+IP+":"+puerto+"/ListaTraslados/",{"TK":clave,"sec":localStorage.mov},function(resultado){
			$.each(miRes1, function(i, elemento){
				html+='<li><p>Codigo:'+elemento.sec+', Identificador: '+elemento.id+'</p></li>';
			});
			html+='</ul></div>';	
		});
		
		$(html).appendTo('#Traslado');
		redireccionar("CTrasladoE.html\#CTrasladoE");
	});
}

//NEW 24/09/2012 Alvaro
function PreNewInventario(){

	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_responsable_0001_consulta_responsable",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html='<label for="id">Identificador:</label><input type="text" name="id" id="id" value=""/>';
		html+='<label for="desc">Descripcion:</label><textarea cols="40" rows="8" name="desc" id="desc"></textarea>';
		html+='<label for="fecha">Fecha:</label><input type="text" name="fecha" id="fecha" placeholder="yyyy-mm-dd" value=""/>';
		html+='<label for="resp">Responsable:</label><select data-theme="b" name="resp" id="resp"><option value="">Seleccione</option>';
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'" >'+elemento.id+'</option>';
		});
		html+='</select>';
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_almacen_0002_consulta_almacen_activo",{"TK":clave},function(resultado1){
			miRes1 = jQuery.parseJSON(resultado1);
			html+='<label for="almacen">Almacen:</label><select data-theme="b" name="almacen" id="almacen"><option value="">Seleccione</option>';
			$.each(miRes1, function(i, elemento){
				html+='<option value="'+elemento.sec+'" >'+elemento.id+'</option>';
			});
			html+='</select><button data-theme="b" onClick="NewInventario()">Crear Inventario</button>';
			$(html).appendTo('#Inventario');
			redireccionar('NewInventario.html\#NewInventario');
		});
	});
}

//NEW 24/09/2012 Alvaro
function NewInventario(){

	select=document.getElementById("resp").options;
	index=document.getElementById("resp").selectedIndex;
	if(index!=0){
		resp=select[index].value;
	}
	select=document.getElementById("almacen").options;
	index=document.getElementById("almacen").selectedIndex;
	if(index!=0){
		almacen=select[index].value;
	}
	
	id=document.getElementById("id").value;
	fecha=document.getElementById("fecha").value;
	desc=document.getElementById("desc").value;
	
	if(id!="" && fecha!="" && resp!="" && almacen!="" ){
		jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_0001_inserta_inventario",{"TK":clave,"id":id,"fecha":fecha,"resp":resp,"almacen":almacen,"desc":desc,"user":localStorage.nombre},function(resultado){
			redireccionar("CInventario1.html");
		});
	}else{
		alert("Error! Los campos Identificador, Fecha, Responsable y Almacén, son obligatorios.");
	}
	
}

//NEW 24/09/2012 Alvaro
function Inventario(){

	html='<label for="matcat">Material:</label><select data-theme="b" name="matcat" id="matcat" onChange="Act()"><option value="">Seleccione</option>';
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_matcat_0001_consulta_matcat",{"TK":clave},function(resultado){	
		miRes = jQuery.parseJSON(resultado);
		$.each(miRes, function(i, elemento){
			html+='<option value="'+elemento.sec+'" >'+elemento.id+'</option>';
		});
		html+='</select><label for="cant">Cantidad:</label><input type="number" name="cant" id="cant" value=""/>';
		html+='</select><button data-theme="b" onClick="RegInventario()">Registrar</button>';
		$(html).appendTo('#Inv');
		redireccionar('Inventario.html\#Inventario');
	});
}

//NEW 24/09/2012 Alvaro
function RegInventario(){
	cant=document.getElementById("cant").value;

	if(matcat!="" && cant!=""){
		jQuery.get("http://"+IP+":"+puerto+"/ValidarInventario",{"TK":clave,"matcat":localStorage.z,"mov":localStorage.mov},function(resultado){	
			miRes = jQuery.parseJSON(resultado);
			if(miRes[0].error=='true'){
				alert("Error! Material ya registrado.");
				return
			}
			jQuery.get("http://"+IP+":"+puerto+"/RegInventario",{"TK":clave,"matcat":localStorage.z,"cant":cant,"user":localStorage.nombre,"mov":localStorage.mov},function(resultado){	
				redireccionar('Inventario.html');		
			});
		
		});
	}else{alert("Error! Los campos Material y Cantidad son obligatorios.");}
}

function Act(){
	select = document.getElementById("matcat").options;
	index = document.getElementById("matcat").selectedIndex;
	localStorage.z=select[index].value;	
}

function CerrarInventario() {
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_inv_movimiento_0003_actualiza_estatus_despacho",{"TK":clave,"sec":localStorage.mov,"user":localStorage.nombre},function(resultado1){
		redireccionar("CInventario1.html");
	});
}

/********************************************************************************************************************************************/
/***                                                          PANTALLAS TIPO UBICACION                                                    ***/
/********************************************************************************************************************************************/

/* Obtiene la lista de Tipos de Ubicacion
 * 2012-09-25 MT
 */
function ConsultaTU(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0002_consulta_tipo_zona",{"TK":clave},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		nro=0;
		html="";
		$.each(miRes, function(i, elemento){
			nro+=1;
			html+='<li><a onClick="DetallesTUsec('+elemento.sec+')"><h3>'+elemento.id+'</h3></a></li>';
		});
		html='<ul data-role="listview" data-inset="true" id="ConsultaCod"><li data-role="list-divider" role="heading"><h3>Tipo Ubicación: '+nro+'</h3></li>'+html+'</ul>';
		$(html).appendTo('#Resp');
		redireccionar("ConsultaTU.html\#CResponsable");
	});	
}

/* Almacena el el localStorage el sec de un Tipo de Ubicacion
 * 2012-09-25 MT
 */
function DetallesTUsec(sec){
	localStorage.tu = sec;
	redireccionar("DetalleTU.html");
}

/* Obtiene los detalles de un Tipo Ubicacion
 * 2012-09-25 MT
 */
function DetallesTU(){
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0003_consulta_tipo_zona_detalle",{"TK":clave,"sec":localStorage.tu},function(resultado){
		miRes = jQuery.parseJSON(resultado);
		html ='';
		$.each(miRes, function(i, elemento){
			html += '<label for="id">Tipo Ubicación:</label><input type="text" name="id" id="id" value="'+elemento.id+'" maxlength="32"/>';
			html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)">'+elemento.desc+'</textarea>';
			if (elemento.activo == 'S') {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
			} else {
				html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S"/>';	
			}
		});
		html += '<a onClick="ActualizarTU('+localStorage.tu+')" data-role="button" data-theme="b">Actualizar</a>';
		$(html).appendTo('#Resp');
		redireccionar("DetalleTU.html\#CResponsable");
	});
}

/* Actualiza los datos de un Tipo Ubicacion
 * 2012-09-25 MT
 */
function ActualizarTU(sec){
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0004_actualiza_tipo_zona",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo,"sec":localStorage.tu},function(resultado){
		redireccionar("DetalleTU.html");
	});
}

/* Carga el formulario de inserción de Tipo Zona
 * 2012-09-25 MT
 */
function NewTU(){
	html = '<label for="id">Tipo Ubicación:</label><input type="text" name="id" id="id" value="" maxlength="32"/>';
	html += '<label for="desc">Descripción:</label><textarea cols="40" rows="8" name="desc" id="desc" onKeyUp="return maximaLongitud(this,100)"></textarea>';
	html += '<label for="activo">Activo </label> <input type="checkbox" onChange="actactivo()" name="activo" id="activo" value="S" checked/>';
	html += '<a onClick="InsertarTU()" data-role="button" data-theme="b">Crear Tipo Ubicación</a>';
	$(html).appendTo('#Resp');
	redireccionar("NewTU.html\#CResponsable");	
}

/* Inserción de Tipo Ubicacion
 * 2012-09-25 MT
 */
function InsertarTU() {
	id = document.getElementById("id").value;
	desc = document.getElementById("desc").value;
	jQuery.get("http://"+IP+":"+puerto+"/t_cor_crm_tipo_zona_0005_inserta_tipo_zona",{"TK":clave,"user":localStorage.nombre,"id":id,"desc":desc,"activo":localStorage.activo},function(resultado){
		redireccionar("ConsultaTU.html");
	});
}
