__author__ = 'palonso0'

import mysql.connector
import csv

cnx=mysql.connector.connect(user='escalon2',password='3scal0n',host='47.60.8.75',database='escalondb2')
cur2=cnx.cursor()

#eri3gdia_top_rssi
file_eri3gdia_top_rssi= open('/var/www/eri3gdia_top_rssi.csv','wb')
wr_eri3gdia_top_rssi= csv.writer(file_eri3gdia_top_rssi,delimiter=';',dialect='excel')
cur2.execute("SELECT 3gdia.celda,cell.NODE_NAME,cell.CONTROLLER_ID,3gdia.fecha,3gdia.3G_ERI_RSSI_dbm FROM 3gdia INNER JOIN cell  ON 3gdia.celda = cell.CELL_NAME where (3gdia.3G_ERI_RSSI_dbm>-100 and (3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (3gdia.celda like 'LE%' or 3gdia.celda like 'ZA%' or 3gdia.celda like 'BU%' or 3gdia.celda like 'PX%' or 3gdia.celda like 'AV%' or 3gdia.celda like 'VA%' or 3gdia.celda like 'SG%' or 3gdia.celda like 'SO%' or 3gdia.celda like 'SA%' or 3gdia.celda like 'NA%' or 3gdia.celda like 'LX%' or 3gdia.celda like 'SX%' or 3gdia.celda like 'S#%' or 3gdia.celda like 'P#%' or 3gdia.celda like 'L#%' )) order by 3gdia.3G_ERI_RSSI_dbm desc;")
rows_eri3gdia_top_rssi=cur2.fetchall()
wr_eri3gdia_top_rssi.writerow(['CELDA','NOMBRE','CONTROLLER','FECHA','RSSI']) 
for fila_eri3gdia_top_rssi in rows_eri3gdia_top_rssi:
   try: wr_eri3gdia_top_rssi.writerow(fila_eri3gdia_top_rssi) 
   except: pass
file_eri3gdia_top_rssi.close()


#hua3gdia_top_rssi
file_hua3gdia_top_rssi= open('/var/www/hua3gdia_top_rssi.csv','wb')
wr_hua3gdia_top_rssi= csv.writer(file_hua3gdia_top_rssi,delimiter=';',dialect='excel')
cur2.execute("SELECT hua3gdia.celda,cell.NODE_NAME,cell.CONTROLLER_ID,hua3gdia.fecha,hua3gdia.3G_HUA_RTWP FROM hua3gdia INNER JOIN cell  ON hua3gdia.celda = cell.CELL_NAME where (hua3gdia.3G_HUA_RTWP>-100 and hua3gdia.3G_HUA_RTWP<0 and (hua3gdia.fecha=(CURDATE()-INTERVAL 1 DAY))  and (hua3gdia.celda like 'LE%' or hua3gdia.celda like 'ZA%' or hua3gdia.celda like 'BU%' or hua3gdia.celda like 'PX%' or hua3gdia.celda like 'AV%' or hua3gdia.celda like 'VA%' or hua3gdia.celda like 'SG%' or hua3gdia.celda like 'SO%' or hua3gdia.celda like 'SA%' or hua3gdia.celda like 'NA%' or hua3gdia.celda like 'LX%' or hua3gdia.celda like 'SX%' or hua3gdia.celda like 'S#%' or hua3gdia.celda like 'P#%' or hua3gdia.celda like 'L#%' )) order by hua3gdia.3G_HUA_RTWP desc;")
rows_hua3gdia_top_rssi=cur2.fetchall()
wr_hua3gdia_top_rssi.writerow(['CELDA','NOMBRE','CONTROLLER','FECHA','RSSI']) 
for fila_hua3gdia_top_rssi in rows_hua3gdia_top_rssi:
   try: wr_hua3gdia_top_rssi.writerow(fila_hua3gdia_top_rssi) 
   except: pass
file_hua3gdia_top_rssi.close()





cur2.close()

