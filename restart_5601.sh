#!/bin/zsh

set +e

pwd=$(cd "$(dirname "$0")" && pwd)

pkill -9 pika

sleep 2

rm -rf /data1/liuyuecai/pika-cloud/data-5601
$pwd/output/pika -c $pwd/conf/pika-5601.conf

sleep 5

ps -ef | grep pika | grep data-5601