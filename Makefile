.PHONY: build push bush

DOCKER_IMAGE=mastak/airflow-metrics

build:
	docker build -t ${DOCKER_IMAGE} .
push:
	docker push ${DOCKER_IMAGE}
bush:
	docker push ${DOCKER_IMAGE}


docker run \
    -v /proc:/host/proc:ro \
    -v /etc/hostname:/host/hostname:ro \
    -e CUSTOM_PROCFS_PATH=/host/proc \
    -e HOSTNAME_PATH=/host/hostname \
    -it -p 8080:8080 \
    --privileged mastak/airflow-metrics

    docker run -it --entrypoint=bash \
        -v /root/airflow-metric:/root/airflow-metric \
        -v /proc:/host/proc:ro \
        -v /etc/hostname:/host/hostname:ro \
        -e CUSTOM_PROCFS_PATH=/host/proc \
        -e HOSTNAME_PATH=/host/hostname \
        -p 8080:8080 \
        --privileged docker.prophy.science/prophy/prophy_backend:a61655a9 -