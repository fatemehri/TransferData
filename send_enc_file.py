import glob
import pyAesCrypt
import os
from ftplib import FTP
import datetime
from pathlib import Path
import shutil


def extract_send():
    ftp = FTP()
    ftp.connect('', 5021)
    ftp.login('admin', '')
    bufferSize = 64 * 1024  # For encrypting files
    password = ""  # For encrypting files
    while 1:
        dt = datetime.datetime.utcnow().date()  # Today Date
        tm = dt + datetime.timedelta(days=1)  # Tomorrow Date
        file = Path("{}.csv".format(dt))  # Filename
        file_aes = Path("{}.csv.aes".format(dt))
        if file_aes.is_file():
            continue
        else:
            # Query command for executing csv file
            h = 'influx -precision rfc3339 -database "grd_db" -username "admin" -password "" ' \
                '-execute "select * from tbl_lvstk_data where ' \
                'time>=\'{}T19:30:00Z\' and time<=\'{}T19:30:00Z\'" ' \
                '-format csv > {}.csv' \
                .format(dt, tm, dt)
            # Export csv file from Influxdb
            os.system(h)
            print('CSV file is ready...', file)

            if os.stat(file).st_size != 0:  # Check file is not empty
                # Encrypt file
                pyAesCrypt.encryptFile(file, "{}".format(file_aes), password, bufferSize)
                print('Encrypted file is ready...', "{}.aes".format(file))
                os.remove("{}".format(file))
                try:
                    # Send file through FTP
                    file_trs = open('{}'.format(file_aes), 'rb')  # file to send
                    ftp.storbinary('STOR %s' % './livestock/{}'.format(file_aes), file_trs)  # send the file
                    file_trs.close()  # close file and FTP
                    # ftp.quit()
                    print('File sent: ', '{} \n {}'.format(file_aes, '-' * 50))

                except Exception as e:
                    print('File did not transfer: ', e)
                    shutil.copyfile('{}'.format(file_aes), 'failedToSend/{}'.format(file_aes))
            else:
                print("File is empty")


if __name__ == '__main__':
    extract_send()  # Call function
