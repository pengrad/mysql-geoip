import gzip
import os
from os.path import join, isfile
import urllib
import zipfile
import MySQLdb
from netaddr import *
from os import listdir
from config import *


def download_files():
    download_dir = 'geo_zip'
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    download_list = [
        'http://geolite.maxmind.com/download/geoip/database/GeoIPCountryCSV.zip',
        'http://geolite.maxmind.com/download/geoip/database/GeoIPv6.csv.gz',
        'http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip',
        'http://geolite.maxmind.com/download/geoip/database/GeoLiteCityv6-beta/GeoLiteCityv6.csv.gz'
    ]
    for url in download_list:
        file_name = url[url.rindex('/') + 1:]
        print 'Download', url, 'in', join(download_dir, file_name)
        urllib.urlretrieve(url, join(download_dir, file_name))


def unzip_files():
    src_dir = 'geo_zip'
    dest_dir = 'geo'
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    for f in [f for f in listdir(src_dir) if isfile(join(src_dir, f))]:
        src_file = join(src_dir, f)
        print 'Extracting', src_file
        if f.endswith('.zip'):
            with zipfile.ZipFile(src_file) as z:
                for zf in z.namelist():
                    if 'GeoLiteCity-Blocks.csv' in zf:
                        global city4, city4_6
                        city4 = join(dest_dir, zf)
                        city4_6 = city4 + prefix_v6
                z.extractall(dest_dir)
        elif f.endswith('csv.gz'):
            dest_file = join(dest_dir, f[:f.rindex(".")])
            with gzip.open(src_file, 'rb') as fin, open(dest_file, 'wb') as fout:
                fout.writelines(fin)


def ip4to6():
    ip6prefix = int(IPAddress("2002::"))
    for file4 in [country4, city4]:
        with open(file4, 'r') as f, open(file4 + prefix_v6, 'w') as f_new:
            print 'Convert to IPv6', file4, 'to', file4 + prefix_v6
            for s in f:
                try:
                    i_first = s.index('","')
                except ValueError:
                    continue
                i_second = s.index('","', i_first + 3)
                ip_s1, ip_s2 = s[1:i_first], s[i_first + 3:i_second]
                ip1, ip2 = IPAddress(ip_s1), IPAddress(ip_s2)
                ip4_1, ip4_2 = int(ip1), int(ip2)
                ip6small_1, ip6small_2 = int(ip1.ipv6()), int(ip2.ipv6())
                ip6big_1, ip6big_2 = ip6prefix + (ip4_1 << 80), ip6prefix + (ip4_2 << 80)

                f_new.write(s)
                f_new.write(s.replace(str(ip4_1), str(ip6small_1)).replace(str(ip4_2), str(ip6small_2)))
                f_new.write(s.replace(str(ip4_1), str(ip6big_1)).replace(str(ip4_2), str(ip6big_2)))


def prepare_db():
    print 'Create database'
    os.system(mysql_cmd + ' < create_db.sql')


def load_csv():
    sql = """
        LOAD DATA LOCAL INFILE '{file}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '{separator}' ENCLOSED BY '"'
        LINES TERMINATED BY '\n' STARTING BY '';
    """
    file2table = [(country4_6, 'csv_country4', ','), (country6, 'csv_country6', ', '), (city4_6, 'csv_city4', ',')]
    db = MySQLdb.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_pass, charset='utf8',
                         db='geoip', local_infile=1)
    cursor = db.cursor()
    for csv_file, table, separator in file2table:
        print 'Load file', csv_file, 'into table', table
        rows = cursor.execute(sql.format(file=os.path.abspath(csv_file), table=table, separator=separator))
        print rows, 'rows'
        db.commit()
    db.close()


def build_db():
    db = MySQLdb.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_pass, charset='utf8',
                         db='geoip')
    cursor = db.cursor()
    print 'Create index on csv_city4(locid)'
    cursor.execute('create index IX__city4__locid on csv_city4(locid);')
    print 'Insert into ip_new from csv_country4'
    rows = cursor.execute("""
        insert ip_new(start, end, cid)
        select t1.start, t1.end, t2.cid
        from csv_country4 t1 join geoip.country t2 on t1.ccode=t2.ccode;
    """)
    print rows, 'rows'
    print 'Insert into ip_new from csv_country6'
    rows = cursor.execute("""
        insert ignore ip_new(start, end, cid)
        select t1.start, t1.end, t2.cid
        from csv_country6 t1 join geoip.country t2 on t1.ccode=t2.ccode;
    """)
    print rows, 'rows'
    print 'Insert into ip_city_new from csv_city4'
    rows = cursor.execute("""
        insert ip_city_new(start, end, cid)
        select t1.start, t1.end, t2.cid
        from csv_city4 t1 join geoip.city_ext t2 on t1.locid=t2.locid;
    """)
    print rows, 'rows'
    db.commit()
    db.close()


def clear_db():
    db = MySQLdb.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_pass, charset='utf8',
                         db='geoip')
    cursor = db.cursor()
    print "Drop tables csv_*"
    cursor.execute("drop table csv_country4, csv_country6, csv_city4")
    db.commit()
    db.close()


def dump_db():
    dump_file = 'geoip_new.sql'
    print 'Dump into file', dump_file
    os.system(mysqldump_cmd + ' geoip ip_new ip_city_new city city_ext > ' + dump_file)


def zip_dump():
    with zipfile.ZipFile('geoip_new.zip', 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write('geoip_new.sql')


def upload_zip():
    print 'To upload file input:'
    print 'scp', os.path.abspath('geoip_new.zip'), 'ssh_path'
    print 'And on server:'
    print 'unzip -o geoip_new.zip'
    print 'mysql -u user -p geoip < geoip_new.sql'


download_files()
unzip_files()
ip4to6()
prepare_db()
load_csv()
build_db()
clear_db()
dump_db()
zip_dump()
upload_zip()
print 'OK'