kill -9  $(sudo lsof -t -i :7000)
kill -9 $(sudo lsof -t -i :7001)
kill -9 $(sudo lsof -t -i :9192)
kill -9 $(sudo lsof -t -i :17070)
kill -9 $(sudo lsof -t -i :11081)

sleep 5

rm -rf /data1/liuyuecai/pika/data

mkdir -p /data1/liuyuecai/pika/data/pika-7000
mkdir -p /data1/liuyuecai/pika/data/pika-7001
mkdir -p /data1/liuyuecai/pika/data/log-7000
mkdir -p /data1/liuyuecai/pika/data/log-7001
mkdir -p /data1/liuyuecai/pika/data/log-codis
mkdir -p /data1/liuyuecai/pika/data/codis

echo 'pika 7000'
./output/pika -c conf/pika-7000.conf

echo 'pika 7001'
./output/pika -c conf/pika-7001.conf

cd codis

make
git checout .

echo 'startup codis dashboard and codis proxy'
./admin/codis-dashboard-admin.sh start
./admin/codis-proxy-admin.sh start
./admin/codis-fe-admin.sh start

sleep 3

CODIS_DASHBOARD_ADDR=wredis24.add.zzdt.qihoo.net:17070
CODIS_GROUP_1_MASTER=wredis24.add.zzdt.qihoo.net:7000
CODIS_GROUP_2_MASTER=wredis24.add.zzdt.qihoo.net:7001

./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --create-group --gid=1
./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --create-group --gid=2

./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --group-add --gid=1 --addr=$CODIS_GROUP_1_MASTER

./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --group-add --gid=2 --addr=$CODIS_GROUP_2_MASTER

./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --slot-action --create-range --beg=0 --end=511 --gid=1
./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --slot-action --create-range --beg=512 --end=1023 --gid=2

echo 'resync all groups'
./bin/codis-admin --dashboard=$CODIS_DASHBOARD_ADDR --resync-group --all

ps -ef | grep pika | grep pika-7000
ps -ef | grep pika | grep pika-7001
ps -ef | grep pika | grep codis | grep data1/liuyuecai