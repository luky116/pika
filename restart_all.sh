kill -9 $(sudo lsof -t -i :7000)
kill -9 $(sudo lsof -t -i :7001)

sleep 3


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
./admin/codis-dashboard-admin.sh restart
./admin/codis-proxy-admin.sh restart
./admin/codis-fe-admin.sh restart

sleep 3


ps -ef | grep pika | grep pika-7000
ps -ef | grep pika | grep pika-7001
ps -ef | grep pika | grep codis | grep data1/liuyuecai