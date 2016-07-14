__author__ = 'palonso0'

import mysql.connector
import csv

cnx=mysql.connector.connect(user='escalon2',password='3scal0n',host='47.60.8.75',database='escalondb2')
cur2=cnx.cursor()

#eri4gdia_top_4g_rrc_fail
file_eri4gdia_top_4g_rrc_fail= open('/home/palonso0/www/eri4gdia_top_4g_rrc_fail.csv','wb')
wr_eri4gdia_4g_rrc_fail= csv.writer(file_eri4gdia_top_4g_rrc_fail,delimiter=';',dialect='excel')
cur2.execute("SELECT  eri4gdia3.celda, cell.SITE_NAME, eri4gdia3.`4G_ERI_RRC_Service_Fail` FROM escalondb2.eri4gdia3 inner join cell on eri4gdia3.celda=cell.CELL_NAME WHERE ((eri4gdia3.fecha=(CURDATE()-INTERVAL 2 DAY) and (eri4gdia3.prov like 'ZA%' OR eri4gdia3.prov like 'BU' OR eri4gdia3.prov like 'LE'  OR eri4gdia3.prov like 'VA'  OR eri4gdia3.prov like 'SG' OR eri4gdia3.prov like 'SO'  OR eri4gdia3.prov like 'SX'  OR eri4gdia3.prov like 'NA'  OR eri4gdia3.prov like 'LX'  OR eri4gdia3.prov like 'SA'  OR eri4gdia3.prov like 'AV'  OR eri4gdia3.prov like 'PX')) and eri4gdia3.`4G_ERI_RRC_Service_Fail` is not null) order by eri4gdia3.`4G_ERI_RRC_Service_Fail` desc;")
rows_eri4gdia_top_4g_rrc_fail=cur2.fetchall()
wr_eri4gdia_4g_rrc_fail.writerow(['CELDA','NOMBRE','RRC FAIL'])
for fila_eri4gdia_top_4g_rrc_fail in rows_eri4gdia_top_4g_rrc_fail:
   try: wr_eri4gdia_4g_rrc_fail.writerow(fila_eri4gdia_top_4g_rrc_fail)
   except: pass
file_eri4gdia_top_4g_rrc_fail.close()


#eri4gdia_top_4g_thr
file_eri4gdia_top_4g_thr= open('/home/palonso0/www/eri4gdia_top_4g_thr.csv','wb')
wr_eri4gdia_top_4g_thr= csv.writer(file_eri4gdia_top_4g_thr,delimiter=';',dialect='excel')
cur2.execute("SELECT  eri4gdia3.celda, cell.SITE_NAME, eri4gdia3.`4G_ERI_DL_Avg_Cell_Throughput_Mbps` FROM escalondb2.eri4gdia3 inner join cell on eri4gdia3.celda=cell.CELL_NAME WHERE ((eri4gdia3.fecha=(CURDATE()-INTERVAL 2 DAY) and (eri4gdia3.prov like 'ZA%' OR eri4gdia3.prov like 'BU' OR eri4gdia3.prov like 'LE'  OR eri4gdia3.prov like 'VA'  OR eri4gdia3.prov like 'SG' OR eri4gdia3.prov like 'SO'  OR eri4gdia3.prov like 'SX'  OR eri4gdia3.prov like 'NA'  OR eri4gdia3.prov like 'LX'  OR eri4gdia3.prov like 'SA'  OR eri4gdia3.prov like 'AV'  OR eri4gdia3.prov like 'PX')) and eri4gdia3.`4G_ERI_DL_Avg_Cell_Throughput_Mbps` is not null) order by eri4gdia3.`4G_ERI_DL_Avg_Cell_Throughput_Mbps` asc;")
rows_eri4gdia_top_4g_thr=cur2.fetchall()
wr_eri4gdia_top_4g_thr.writerow(['CELDA','NOMBRE','THR'])
for fila_eri4gdia_top_4g_thr in rows_eri4gdia_top_4g_thr:
   try: wr_eri4gdia_top_4g_thr.writerow(fila_eri4gdia_top_4g_thr)
   except: pass
file_eri4gdia_top_4g_thr.close()





cur2.close()

