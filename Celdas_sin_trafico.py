__author__ = 'palonso0'

import mysql.connector
import csv

cnx=mysql.connector.connect(user='escalon2',password='3scal0n',host='47.60.8.75',database='escalondb2')
cur2=cnx.cursor()

#eri3gdia_sinhsdpa
file_eri3ghsdpa= open('/var/www/eri3gdia_sinhsdpa.csv','wb')
wr_eri3ghsdpa= csv.writer(file_eri3ghsdpa,delimiter=';',dialect='excel')
cur2.execute("SELECT 3gdia.celda,cell.NODE_NAME FROM 3gdia INNER JOIN cell  ON 3gdia.celda = cell.CELL_NAME where (3gdia.3G_GB_HSDPA_DL='0' and (3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (3gdia.celda like 'LE%' or 3gdia.celda like 'ZA%' or 3gdia.celda like 'BU%' or 3gdia.celda like 'PX%' or 3gdia.celda like 'AV%' or 3gdia.celda like 'VA%' or 3gdia.celda like 'SG%' or 3gdia.celda like 'SO%' or 3gdia.celda like 'SA%' or 3gdia.celda like 'NA%' or 3gdia.celda like 'LX%' or 3gdia.celda like 'SX%' ));")
rows_eri3ghsdpa=cur2.fetchall()
wr_eri3ghsdpa.writerow(['CELDA','NOMBRE']) 
for fila_eri3ghsdpa in rows_eri3ghsdpa:
   try: wr_eri3ghsdpa.writerow(fila_eri3ghsdpa) 
   except: pass
file_eri3ghsdpa.close()

#hua4gdia_sindatos
file_hua4gdia= open('/var/www/hua4gdia_sindatos.csv','wb')
wr_hua4gdia= csv.writer(file_hua4gdia,delimiter=';',dialect='excel')
cur2.execute("SELECT hua4gdia.celda,cell.NODE_NAME FROM hua4gdia INNER JOIN cell  ON hua4gdia.celda = cell.CELL_NAME where (hua4gdia.4G_HUA_MB_DL_Traffic='0' and (hua4gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (hua4gdia.celda like 'LE%' or hua4gdia.celda like 'ZA%' or hua4gdia.celda like 'BU%' or hua4gdia.celda like 'PX%' or hua4gdia.celda like 'AV%' or hua4gdia.celda like 'VA%' or hua4gdia.celda like 'SG%' or hua4gdia.celda like 'SO%' or hua4gdia.celda like 'SA%' or hua4gdia.celda like 'NA%' or hua4gdia.celda like 'LX%' or hua4gdia.celda like 'SX%' ) and celda not like '%J%');")
rows_hua4gdia=cur2.fetchall()
wr_hua4gdia.writerow(['CELDA','NOMBRE']) 
for fila_hua4gdia in rows_hua4gdia:
   try: wr_hua4gdia.writerow(fila_hua4gdia) 
   except: pass
file_hua4gdia.close()

#eri4gdia_sindatos
file_eri4gdia= open('/var/www/eri4gdia_sindatos.csv','wb')
wr_eri4gdia= csv.writer(file_eri4gdia,delimiter=';',dialect='excel')
cur2.execute("SELECT eri4gdia3.celda,cell.NODE_NAME FROM eri4gdia3 INNER JOIN cell  ON eri4gdia3.celda = cell.CELL_NAME where (eri4gdia3.4G_ERI_MB_DL_Traffic='0' and (eri4gdia3.fecha=(CURDATE()-INTERVAL 1 DAY))  and (eri4gdia3.celda like 'LE%' or eri4gdia3.celda like 'ZA%' or eri4gdia3.celda like 'BU%' or eri4gdia3.celda like 'PX%' or eri4gdia3.celda like 'AV%' or eri4gdia3.celda like 'VA%' or eri4gdia3.celda like 'SG%' or eri4gdia3.celda like 'SO%' or eri4gdia3.celda like 'SA%' or eri4gdia3.celda like 'NA%' or eri4gdia3.celda like 'LX%' or eri4gdia3.celda like 'SX%' ) and celda not like '%J%');")
rows_eri4gdia=cur2.fetchall()
wr_eri4gdia.writerow(['CELDA','NOMBRE']) 
for fila_eri4gdia in rows_eri4gdia:
   try: wr_eri4gdia.writerow(fila_eri4gdia) 
   except: pass
file_eri4gdia.close()

#hua3gdia_sinvoz
file_hua3gdia_sinvoz= open('/var/www/hua3gdia_sinvoz.csv','wb')
wr_hua3gdia_sinvoz= csv.writer(file_hua3gdia_sinvoz,delimiter=';',dialect='excel')
cur2.execute("SELECT hua3gdia.celda,cell.NODE_NAME FROM hua3gdia INNER JOIN cell ON hua3gdia.celda = cell.CELL_NAME where (hua3gdia.3G_Minutes_Voice='0' and (hua3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (hua3gdia.celda like 'LE%' or hua3gdia.celda like 'ZA%' or hua3gdia.celda like 'BU%' or hua3gdia.celda like 'PX%' or hua3gdia.celda like 'AV%' or hua3gdia.celda like 'VA%' or hua3gdia.celda like 'SG%' or hua3gdia.celda like 'SO%' or hua3gdia.celda like 'SA%' or hua3gdia.celda like 'NA%' or hua3gdia.celda like 'LX%' or hua3gdia.celda like 'SX%' ));")
rows_hua3gdia_sinvoz=cur2.fetchall()
wr_hua3gdia_sinvoz.writerow(['CELDA','NOMBRE']) 
for fila_hua3gdia_sinvoz in rows_hua3gdia_sinvoz:
   try: wr_hua3gdia_sinvoz.writerow(fila_hua3gdia_sinvoz) 
   except: pass
file_hua3gdia_sinvoz.close()


#hua3gdia_sindatos
file_hua3gdia_sindatos= open('/var/www/hua3gdia_sindatos.csv','wb')
wr_hua3gdia_sindatos= csv.writer(file_hua3gdia_sindatos,delimiter=';',dialect='excel')
cur2.execute("SELECT celda,cell.NODE_NAME FROM hua3gdia INNER JOIN cell ON hua3gdia.celda = cell.CELL_NAME where (hua3gdia.3G_GB_HSDPA_DL='0' and (hua3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (hua3gdia.celda like 'LE%' or hua3gdia.celda like 'ZA%' or hua3gdia.celda like 'BU%' or hua3gdia.celda like 'PX%' or hua3gdia.celda like 'AV%' or hua3gdia.celda like 'VA%' or hua3gdia.celda like 'SG%' or hua3gdia.celda like 'SO%' or hua3gdia.celda like 'SA%' or hua3gdia.celda like 'NA%' or hua3gdia.celda like 'LX%' or hua3gdia.celda like 'SX%' ));")
rows_hua3gdia_sindatos=cur2.fetchall()
wr_hua3gdia_sindatos.writerow(['CELDA','NOMBRE']) 
for fila_hua3gdia_sindatos in rows_hua3gdia_sindatos:
   try: wr_hua3gdia_sindatos.writerow(fila_hua3gdia_sindatos) 
   except: pass
file_hua3gdia_sindatos.close()


#eri3g_sinvoz
file_eri3g_sinvoz= open('/var/www/eri3g_sinvoz.csv','wb')
wr_eri3g_sinvoz= csv.writer(file_eri3g_sinvoz,delimiter=';',dialect='excel')
cur2.execute("SELECT celda,cell.NODE_NAME FROM 3gdia INNER JOIN cell  ON 3gdia.celda = cell.CELL_NAME where (3gdia.3G_Minutes_Voice='0' and (3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (3gdia.celda like 'LE%' or 3gdia.celda like 'ZA%' or 3gdia.celda like 'BU%' or 3gdia.celda like 'PX%' or 3gdia.celda like 'AV%' or 3gdia.celda like 'VA%' or 3gdia.celda like 'SG%' or 3gdia.celda like 'SO%' or 3gdia.celda like 'SA%' or 3gdia.celda like 'NA%' or 3gdia.celda like 'LX%' or 3gdia.celda like 'SX%' ));")
rows_eri3g_sinvoz=cur2.fetchall()
wr_eri3g_sinvoz.writerow(['CELDA','NOMBRE']) 
for fila_eri3g_sinvoz in rows_eri3g_sinvoz:
   try: wr_eri3g_sinvoz.writerow(fila_eri3g_sinvoz) 
   except: pass
file_eri3g_sinvoz.close()

#eri2g_sinvoz
file_eri2g_sinvoz= open('/var/www/eri2g_sinvoz.csv','wb')
wr_eri2g_sinvoz= csv.writer(file_eri2g_sinvoz,delimiter=';',dialect='excel')
cur2.execute("SELECT eri2gdia.celda,cell.NODE_NAME FROM eri2gdia INNER JOIN cell  ON eri2gdia.celda = cell.CELL_NAME where (eri2gdia.2G_ERI_Minutes='0' and (eri2gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (eri2gdia.celda like 'LE%' or eri2gdia.celda like 'ZA%' or eri2gdia.celda like 'BU%' or eri2gdia.celda like 'PX%' or eri2gdia.celda like 'AV%' or eri2gdia.celda like 'VA%' or eri2gdia.celda like 'SG%' or eri2gdia.celda like 'SO%' or eri2gdia.celda like 'SA%' or eri2gdia.celda like 'NA%' or eri2gdia.celda like 'LX%' or eri2gdia.celda like 'SX%' ));")
rows_eri2g_sinvoz=cur2.fetchall()
wr_eri2g_sinvoz.writerow(['CELDA','NOMBRE']) 
for fila_eri2g_sinvoz in rows_eri2g_sinvoz:
   try: wr_eri2g_sinvoz.writerow(fila_eri2g_sinvoz) 
   except: pass
file_eri2g_sinvoz.close()

#hua2g_sinvoz
file_hua2g_sinvoz= open('/var/www/hua2g_sinvoz.csv','wb')
wr_hua2g_sinvoz= csv.writer(file_hua2g_sinvoz,delimiter=';',dialect='excel')
cur2.execute("SELECT hua2gdia.celda,cell.NODE_NAME FROM hua2gdia INNER JOIN cell  ON hua2gdia.celda = cell.CELL_NAME where (hua2gdia.2G_HUA_Minutes_Time='0' and (hua2gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (hua2gdia.celda like 'LE%' or hua2gdia.celda like 'ZA%' or hua2gdia.celda like 'BU%' or hua2gdia.celda like 'PX%' or hua2gdia.celda like 'AV%' or hua2gdia.celda like 'VA%' or hua2gdia.celda like 'SG%' or hua2gdia.celda like 'SO%' or hua2gdia.celda like 'SA%' or hua2gdia.celda like 'NA%' or hua2gdia.celda like 'LX%' or hua2gdia.celda like 'SX%' ));")
rows_hua2g_sinvoz=cur2.fetchall()
wr_hua2g_sinvoz.writerow(['CELDA','NOMBRE']) 
for fila_hua2g_sinvoz in rows_hua2g_sinvoz:
   try: wr_hua2g_sinvoz.writerow(fila_hua2g_sinvoz) 
   except: pass
file_hua2g_sinvoz.close()

#sie2g_sinvoz
file_sie2g_sinvoz= open('/var/www/sie2g_sinvoz.csv','wb')
wr_sie2g_sinvoz= csv.writer(file_sie2g_sinvoz,delimiter=';',dialect='excel')
cur2.execute("SELECT sie2gdia.celda,cell.NODE_NAME FROM sie2gdia INNER JOIN cell  ON sie2gdia.celda = cell.CELL_NAME where (sie2gdia.2G_SIE_Minutes='0' and (sie2gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (sie2gdia.celda like 'LE%' or sie2gdia.celda like 'ZA%' or sie2gdia.celda like 'BU%' or sie2gdia.celda like 'PX%' or sie2gdia.celda like 'AV%' or sie2gdia.celda like 'VA%' or sie2gdia.celda like 'SG%' or sie2gdia.celda like 'SO%' or sie2gdia.celda like 'SA%' or sie2gdia.celda like 'NA%' or sie2gdia.celda like 'LX%' or sie2gdia.celda like 'SX%' or sie2gdia.celda like 'S#%' or sie2gdia.celda like 'P#%' or sie2gdia.celda like 'L#%' ));")
rows_sie2g_sinvoz=cur2.fetchall()
wr_sie2g_sinvoz.writerow(['CELDA','NOMBRE']) 
for fila_sie2g_sinvoz in rows_sie2g_sinvoz:
   try: wr_sie2g_sinvoz.writerow(fila_sie2g_sinvoz) 
   except: pass
file_sie2g_sinvoz.close()



cur2.close()

