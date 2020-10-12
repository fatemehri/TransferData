import glob
import pyAesCrypt
import os
from ftplib import FTP
import datetime
from pathlib import Path
import shutil


def extract_send():
    ftp = FTP()
    # session = ftplib.FTP('ftp://partow.rdps.ir:5021/', 'admin', 'A@123456')  # Connect to FTP server
    ftp.connect('', 5021)
    ftp.login('admin', '')
    bufferSize = 64 * 1024  # For encrypting files
    password = ""  # For encrypting files
    while 1:
        dt = datetime.datetime.utcnow().date()  # Today Date
        yd = dt - datetime.timedelta(days=1)  # Yesterday Date
        file_garden = Path("garden{}.csv".format(dt))  # Filename
        file_livestock = Path("livestock{}.csv".format(dt))  # Filename
        file_aes_garden = Path("garden{}.csv.aes".format(dt))
        file_aes_livestock = Path("livestock{}.csv.aes".format(dt))
        if str(datetime.datetime.now().time()).split('.')[0] == '03:00:00':
            if file_aes_garden.is_file() and file_aes_livestock.is_file():
                continue
            else:
                # Query command for executing csv file
                livestock = 'influx -precision rfc3339 -database "grd_db" -username "admin" -password "" ' \
                    '-execute "select * from tbl_lvstk_data where ' \
                    'time>=\'{}T00:00:00Z\' and time<=\'{}T00:00:00Z\'" ' \
                    '-format csv > livestock{}.csv' \
                    .format('2020-09-01', '2020-09-02', dt)
                garden = 'influx -precision rfc3339 -database "grd_db" -username "admin" -password "" ' \
                    '-execute "select * from tbl_frs_data where ' \
                    'time>=\'{}T00:00:00Z\' and time<=\'{}T00:00:00Z\'" ' \
                    '-format csv > garden{}.csv' \
                    .format('2020-09-01', '2020-09-02', dt)
                # Export csv file from Influxdb
                os.system(livestock)
                os.system(garden)
                print('CSV files is ready...', file_garden)
                print('CSV files is ready...', file_livestock)

                for name in glob.glob('*.csv'):
                    if os.stat(name).st_size != 0:  # Check file is not empty
                        # Encrypt file
                        pyAesCrypt.encryptFile(name, "{}.aes".format(name), password, bufferSize)
                        print('Encrypted file is ready...', "{}.aes".format(name))
                        os.remove("{}".format(name))
                        try:
                            # Send file through FTP
                            file_trs = open('{}.aes'.format(name), 'rb')  # file to send
                            ftp.storbinary('STOR %s' % './livestock/{}.aes'.format(name), file_trs)  # send the file
                            file_trs.close()  # close file and FTP
                            # ftp.quit()
                            print('File sent: ', '{}.aes \n {}'.format(name, '-' * 50))

                        except Exception as e:
                            print('File did not transfer: ', e)
                            shutil.copyfile('{}.aes'.format(name), 'failedToSend/{}.aes'.format(name))
                    else:
                        print("File is empty")


if __name__ == '__main__':
    extract_send()  # Call function
