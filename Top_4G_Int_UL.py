__author__ = 'palonso0'

import mysql.connector
import csv

cnx=mysql.connector.connect(user='escalon2',password='3scal0n',host='47.60.8.75',database='escalondb2')
cur2=cnx.cursor()

#eri4gdia_top_rssi
file_eri4gdia_top_rssi= open('/var/www/eri4gdia_top_interf.csv','wb')
wr_eri4gdia_top_rssi= csv.writer(file_eri4gdia_top_rssi,delimiter=';',dialect='excel')
cur2.execute("SELECT eri4gdia3.fecha,eri4gdia3.prov,eri4gdia3.celda,cell.NODE_NAME,eri4gdia3.`4G_ERI_UL_PUCCH_Interference_Avg` FROM escalondb2.eri4gdia3 INNER JOIN cell ON eri4gdia3.celda = cell.CELL_NAME  where eri4gdia3.prov in ('AV','BU','SG','SA','S','SO','LE','LO','P','VA','ZA','NA','LO') and eri4gdia3.fecha=(CURDATE()-INTERVAL 1 DAY) AND eri4gdia3.`4G_ERI_UL_PUCCH_Interference_Avg`>-110 order by eri4gdia3.`4G_ERI_UL_PUCCH_Interference_Avg` desc;")
rows_eri4gdia_top_rssi=cur2.fetchall()
wr_eri4gdia_top_rssi.writerow(['FECHA','PROV','CELDA','NOMBRE','INTERF MEDIA','ZONA'])
for fila_eri4gdia_top_rssi in rows_eri4gdia_top_rssi:
   if fila_eri4gdia_top_rssi[1] in ['VA','BU','AV','SG','SO','LO','NA'] : fila_eri4gdia_top_rssi=fila_eri4gdia_top_rssi+('ROJA',)
   if (fila_eri4gdia_top_rssi[1] in ['SX','SA','ZA','LE','PX'] and 'W' in fila_eri4gdia_top_rssi[2]  ) : fila_eri4gdia_top_rssi=fila_eri4gdia_top_rssi+('NARANJA OR',)
   if (fila_eri4gdia_top_rssi[1] in ['SX','SA','ZA','LE','PX'] and 'I' in fila_eri4gdia_top_rssi[2]  ) : fila_eri4gdia_top_rssi=fila_eri4gdia_top_rssi+('NARANJA OR',)
   if (fila_eri4gdia_top_rssi[1] in ['SX','SA','ZA','LE','PX'] and 'W' not in fila_eri4gdia_top_rssi[2] and 'I' not in fila_eri4gdia_top_rssi[2]    ) : fila_eri4gdia_top_rssi=fila_eri4gdia_top_rssi+('NARANJA VF',)
   try: wr_eri4gdia_top_rssi.writerow(fila_eri4gdia_top_rssi)
   except: pass
file_eri4gdia_top_rssi.close()


#hua4gdia_top_rssi
file_hua4gdia_top_rssi= open('/var/www/hua4gdia_top_interf.csv','wb')
wr_hua4gdia_top_rssi= csv.writer(file_hua4gdia_top_rssi,delimiter=';',dialect='excel')
cur2.execute("SELECT hua4gdia.fecha,hua4gdia.prov,hua4gdia.celda,cell.NODE_NAME,hua4gdia.`4G_HUA_Avg_UL_Interference` FROM escalondb2.hua4gdia INNER JOIN cell ON hua4gdia.celda=cell.CELL_NAME where hua4gdia.prov in ('AV','BU','SG','SA','S','SO','LE','LO','P','VA','ZA','NA','LO') and hua4gdia.fecha=(CURDATE()-INTERVAL 1 DAY) AND hua4gdia.`4G_HUA_Avg_UL_Interference`>-110 order by hua4gdia.`4G_HUA_Avg_UL_Interference` desc  ;")
rows_hua4gdia_top_rssi=cur2.fetchall()
wr_hua4gdia_top_rssi.writerow(['FECHA','PROV','CELDA','NOMBRE','INTERF_MEDIA'])
for fila_hua4gdia_top_rssi in rows_hua4gdia_top_rssi:
   try: wr_hua4gdia_top_rssi.writerow(fila_hua4gdia_top_rssi)
   except: pass
file_hua4gdia_top_rssi.close()





cur2.close()
