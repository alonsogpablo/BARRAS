#!/usr/bin/env python
import mysql.connector
import csv
from datetime import date, timedelta

cnx=mysql.connector.connect(user='escalon2',password='3scal0n',host='47.60.8.75',database='escalondb2')
cur=cnx.cursor()


def genera_csv_highcharts(tabla,kpi,lim):

    #Abrimos el fichero para exportar

    if tabla=='hua3gdia':estad = open("/var/www/estad_cyl_3ghua_%s.csv" %kpi,'wb')
    else:estad = open("/var/www/estad_cyl_%s.csv" % kpi,'wb')
    
    wr = csv.writer(estad,delimiter=';',dialect='excel')

    cabecera=[""]
    cabecera[0]="fecha"
    cabecera.append(date.today()-timedelta(3))
    cabecera.append(date.today()-timedelta(2))
    cabecera.append(date.today()-timedelta(1))

    wr.writerow(cabecera)

    if tabla=='3gdia':
        query="SELECT 3gdia.fecha, cell.NODE_NAME, Sum(3gdia.%s)  FROM cell INNER JOIN 3gdia ON cell.CELL_NAME = 3gdia.celda WHERE (3gdia.fecha=(CURDATE()-INTERVAL 3 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query)
        query_f1=cur.fetchall()

        query2="SELECT 3gdia.fecha, cell.NODE_NAME, Sum(3gdia.%s)  FROM cell INNER JOIN 3gdia ON cell.CELL_NAME = 3gdia.celda WHERE (3gdia.fecha=(CURDATE()-INTERVAL 2 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query2)
        query_f2=cur.fetchall()

        query3="SELECT 3gdia.fecha, cell.NODE_NAME, Sum(3gdia.%s)  FROM cell INNER JOIN 3gdia ON cell.CELL_NAME = 3gdia.celda WHERE (3gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query3)
        query_f3=cur.fetchall()

    elif tabla== 'sie2gdia':
        query="SELECT sie2gdia.fecha, cell.NODE_NAME, Sum(sie2gdia.%s)  FROM cell INNER JOIN sie2gdia ON cell.CELL_NAME = sie2gdia.celda WHERE (sie2gdia.fecha=(CURDATE()-INTERVAL 3 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(sie2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query)
        query_f1=cur.fetchall()

        query2="SELECT sie2gdia.fecha, cell.NODE_NAME, Sum(sie2gdia.%s)  FROM cell INNER JOIN sie2gdia ON cell.CELL_NAME = sie2gdia.celda WHERE (sie2gdia.fecha=(CURDATE()-INTERVAL 2 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(sie2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query2)
        query_f2=cur.fetchall()

        query3="SELECT sie2gdia.fecha, cell.NODE_NAME, Sum(sie2gdia.%s)  FROM cell INNER JOIN sie2gdia ON cell.CELL_NAME = sie2gdia.celda WHERE (sie2gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(sie2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query3)
        query_f3=cur.fetchall()

    elif tabla== 'eri2gdia':
        query="SELECT eri2gdia.fecha, cell.NODE_NAME, Sum(eri2gdia.%s)  FROM cell INNER JOIN eri2gdia ON cell.CELL_NAME = eri2gdia.celda WHERE (eri2gdia.fecha=(CURDATE()-INTERVAL 3 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(eri2gdia.%s)>%d);" % (kpi,kpi,lim)
        cur.execute(query)
        query_f1=cur.fetchall()

        query2="SELECT eri2gdia.fecha, cell.NODE_NAME, Sum(eri2gdia.%s)  FROM cell INNER JOIN eri2gdia ON cell.CELL_NAME = eri2gdia.celda WHERE (eri2gdia.fecha=(CURDATE()-INTERVAL 2 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(eri2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query2)
        query_f2=cur.fetchall()

        query3="SELECT eri2gdia.fecha, cell.NODE_NAME, Sum(eri2gdia.%s)  FROM cell INNER JOIN eri2gdia ON cell.CELL_NAME = eri2gdia.celda WHERE (eri2gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(eri2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query3)
        query_f3=cur.fetchall()

    elif tabla=='hua2gdia':
        query="SELECT hua2gdia.fecha, cell.NODE_NAME, Sum(hua2gdia.%s)  FROM cell INNER JOIN hua2gdia ON cell.CELL_NAME = hua2gdia.celda WHERE (hua2gdia.fecha=(CURDATE()-INTERVAL 3 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query)
        query_f1=cur.fetchall()

        query2="SELECT hua2gdia.fecha, cell.NODE_NAME, Sum(hua2gdia.%s)  FROM cell INNER JOIN hua2gdia ON cell.CELL_NAME = hua2gdia.celda WHERE (hua2gdia.fecha=(CURDATE()-INTERVAL 2 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query2)
        query_f2=cur.fetchall()

        query3="SELECT hua2gdia.fecha, cell.NODE_NAME, Sum(hua2gdia.%s)  FROM cell INNER JOIN hua2gdia ON cell.CELL_NAME = hua2gdia.celda WHERE (hua2gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO' OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua2gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query3)
        query_f3=cur.fetchall()

    elif tabla=='hua3gdia':
        query="SELECT hua3gdia.fecha, cell.NODE_NAME, Sum(hua3gdia.%s)  FROM cell INNER JOIN hua3gdia ON cell.CELL_NAME = hua3gdia.celda WHERE (hua3gdia.fecha=(CURDATE()-INTERVAL 3 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO'  OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query)
        query_f1=cur.fetchall()

        query2="SELECT hua3gdia.fecha, cell.NODE_NAME, Sum(hua3gdia.%s)  FROM cell INNER JOIN hua3gdia ON cell.CELL_NAME = hua3gdia.celda WHERE (hua3gdia.fecha=(CURDATE()-INTERVAL 2 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO'  OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query2)
        query_f2=cur.fetchall()

        query3="SELECT hua3gdia.fecha, cell.NODE_NAME, Sum(hua3gdia.%s)  FROM cell INNER JOIN hua3gdia ON cell.CELL_NAME = hua3gdia.celda WHERE (hua3gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND (cell.PROVINCE='VA' OR cell.PROVINCE='LE' OR cell.PROVINCE='SA' OR cell.PROVINCE='SG' OR cell.PROVINCE='SO' OR cell.PROVINCE='NA'  OR cell.PROVINCE='AV' OR cell.PROVINCE='ZA'  OR cell.PROVINCE='LO'  OR cell.PROVINCE='BU' OR cell.PROVINCE='P' OR cell.PROVINCE='S')) GROUP BY cell.NODE_NAME HAVING (Sum(hua3gdia.%s)>%d) ORDER BY cell.NODE_NAME;" % (kpi,kpi,lim)
        cur.execute(query3)
        query_f3=cur.fetchall()

    #Para el d?a 1:
    dict_nodos_dia1={}                      #Aqu? guardamos los pares nodo-kpi del d?a 1
    for lista1 in query_f1:
        registro1=[""]                      #Aqu? guardaremos las l?neas que ir?n al csv con datos s?lo del d?a 1
        dict_nodos_dia1[lista1[1]]=lista1[2]
	
    #Para el d?a 2:
    dict_nodos_dia2={}                       #Aqu? guardamos los pares nodo-kpi del d?a 2
    for lista2 in query_f2:
        registro2=[""]
        dict_nodos_dia2[lista2[1]]=lista2[2]

    #Para el d?a 3:
    dict_nodos_dia3={}
    for lista3 in query_f3:
        registro3=[""]
        dict_nodos_dia3[lista3[1]]=lista3[2]


    #Construimos el registro1 con los nodos que aparecen s?lo el d?a 1, en 1 y 2 y en 1,2 y 3:
    for nodo in dict_nodos_dia1:
        registro1[0]=str(nodo)
        registro1.append(str(dict_nodos_dia1[nodo]))
        if nodo in dict_nodos_dia2:registro1.append(str(dict_nodos_dia2[nodo]))
        if nodo in dict_nodos_dia3:registro1.append(str(dict_nodos_dia3[nodo]))
        wr.writerow(registro1)
        registro1=[""]

    #A?adimos los registros no presentes el d?a 1 pero s? el 2 ? el 2 y 3
    for nodo in dict_nodos_dia2:
        if nodo not in dict_nodos_dia1:
            registro2[0]=str(nodo)
            registro2.append("0")
            registro2.append(str(dict_nodos_dia2[nodo]))
            if nodo in dict_nodos_dia3:registro2.append(str(dict_nodos_dia3[nodo]))
            wr.writerow(registro2)
            registro2=[""]

    #A?adimos los nodos solo presentes el d?a 3:
    for nodo in dict_nodos_dia3:
        if nodo not in dict_nodos_dia1:
            if nodo not in dict_nodos_dia2:
                registro3[0]=str(nodo)
                registro3.append(str("0"))
                registro3.append(str("0"))
                registro3.append(str(dict_nodos_dia3[nodo]))
                wr.writerow(registro3)
                registro3=[""]

    #A?adimos los nodos solo presentes los d?as 1 y 3:
    for nodo in dict_nodos_dia3:
        if nodo in dict_nodos_dia1:
            if nodo not in dict_nodos_dia2:
                registro3[0]=str(nodo)
                registro3.append(str(dict_nodos_dia1[nodo]))
                registro3.append(str("0"))
                registro3.append(str(dict_nodos_dia3[nodo]))
                wr.writerow(registro3)
                registro3=[""]


    estad.close()


try:genera_csv_highcharts('3gdia','3G_RAB_Fail_CS',10)
except:pass
try:genera_csv_highcharts('3gdia','3G_Drop_Voice',5)
except:pass
try:genera_csv_highcharts('3gdia','3G_RAB_Fail_PS',10)
except:pass
try:genera_csv_highcharts('3gdia','3G_RRC_Fail_CS',10)
except:pass
try:genera_csv_highcharts('3gdia','3G_RRC_Fail_PS',10)
except:pass
try:genera_csv_highcharts('eri2gdia','2G_ERI_Call_Bloq',10)
except:pass
try:genera_csv_highcharts('eri2gdia','2G_ERI_Call_Drops',5)
except:pass
try:genera_csv_highcharts('sie2gdia','2G_SIE_Call_Bloq_Real',0)
except:pass
try:genera_csv_highcharts('sie2gdia','2G_SIE_Call_Drops',10)
except:pass
try:genera_csv_highcharts('hua2gdia','2G_HUA_Call_Drop',10)
except:pass
try:genera_csv_highcharts('hua2gdia','2G_HUA_Call_Blocks',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_RAB_Fail_CS',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_Drop_Voice',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_RAB_Fail_PS',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_RRC_Fail_CS',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_RRC_Fail_PS',10)
except:pass
try:genera_csv_highcharts('hua3gdia','3G_DT_Diario',50)
except:pass
try:genera_csv_highcharts('3gdia','3G_DT_Diario',50)
except:pass


