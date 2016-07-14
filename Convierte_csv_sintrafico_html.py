import csv

def csvtohtml(fichero):

    reader = csv.reader(open(fichero+'.csv'),delimiter=';')
    # Create the HTML file for output
    htmlfile = open(fichero+'.html',"w")
    rownum = 0
    # write <table> tag
    htmlfile.write('<!DOCTYPE html><html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;}th,td {padding: 15px;}</style></head><body>')
    htmlfile.write('<table>')
    # generate table contents
    for row in reader: # Read a single row from the CSV file
    # write header row. assumes first row in csv contains header
        if rownum == 0:
            htmlfile.write('<tr>') # write <tr> tag
            for column in row:
                htmlfile.write('<th>' + column + '</th>')
            htmlfile.write('</tr>')
    #write all other rows
        else:
            htmlfile.write('<tr>')
            for column in row:
                htmlfile.write('<td>' + column + '</td>')
            htmlfile.write('</tr>')
    #increment row count
        rownum += 1
    # write </table> tag
    htmlfile.write('</table>')
    # print results to shell
    print "Created " + str(rownum) + " row table."
    #exit(0)

    return

csvtohtml('/var/www/eri3g_sinvoz');
csvtohtml('/var/www/eri3gdia_sinhsdpa');
csvtohtml('/var/www/eri4gdia_sindatos');
csvtohtml('/var/www/hua3gdia_sindatos');
csvtohtml('/var/www/hua4gdia_sindatos');
csvtohtml('/var/www/hua3gdia_sinvoz');
csvtohtml('/var/www/hua2g_sinvoz');
csvtohtml('/var/www/eri2g_sinvoz');
csvtohtml('/var/www/sie2g_sinvoz');