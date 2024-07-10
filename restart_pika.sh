if [ "$1" -eq 1 ]; then
  cd /data01/yuecai/rocksdb
  git pull

  rm -rf /data01/yuecai/pika-start-time/buildtrees/Source/rocksdb/db
  rm -rf /data01/yuecai/pika-start-time/buildtrees/Source/rocksdb/cloud
  cp -rf /data01/yuecai/rocksdb/db /data01/yuecai/pika-start-time/buildtrees/Source/rocksdb
  cp -rf /data01/yuecai/rocksdb/cloud /data01/yuecai/pika-start-time/buildtrees/Source/rocksdb
  echo "update rocksdb success"

  cd /data01/yuecai/pika-start-time
  export USE_S3=1
  ./build.sh
  echo "rebuild pika success"
fi

cd /data01/yuecai/pika-start-time

kill -9 $(lsof -t -i :3344)
rm -rf ./pika-cloud2

nohup ./output/pika -c conf/pika-3344-test.conf > start_log.out &