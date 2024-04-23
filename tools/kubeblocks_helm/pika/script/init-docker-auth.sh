# docker 授权：https://k8s.qihoo.net/portal/namespace/339/container-image?registryType=namespace
#kubectl create secret docker-registry qihoo-secret --docker-server=harbor.qihoo.net/jcjgz-pulsar --docker-username=jcjgz-pulsar --docker-password=5Lx1y3H3gNSx1s5Kmh -n $k8s_namespace

# docker 授权：https://k8s.qihoo.net/portal/namespace/339/container-image?registryType=namespace
secret_name="qihoo-secret"
docker_hub_url="harbor.qihoo.net/jcjgz-pika"
docker_nuername="jcjgz-pika"
docker_password="bUf673Y1ZHCgplK5XM"

kubectl create secret docker-registry $secret_name \
    --docker-server=$docker_hub_url \
    --docker-username=$docker_nuername \
    --docker-password=$docker_password