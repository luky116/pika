rm -rf /data1/liuyuecai/pika/data/pika-7000
rm -rf /data1/liuyuecai/pika/data/pika-7001
rm -rf /data1/liuyuecai/pika/data/codis

mkdir -p /data1/liuyuecai/pika/data/pika-7000
mkdir -p /data1/liuyuecai/pika/data/pika-7001
mkdir -p /data1/liuyuecai/pika/data/codis

echo 'pika 7000'
./output/pika -c conf/pika-7000.conf

echo 'pika 7001'
./output/pika -c conf/pika-7001.conf

cd codis

echo 'startup codis dashboard and codis proxy'
./admin/codis-dashboard-admin.sh start
./admin/codis-proxy-admin.sh start
./admin/codis-fe-admin.sh start

sleep 3


ps -ef | grep pika | grep pika-7000
ps -ef | grep pika | grep pika-7001
ps -ef | grep pika | grep codis | grep data1/liuyuecai