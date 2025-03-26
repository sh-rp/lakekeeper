run-keycloak:
	cd keycloak && docker build -t keycloak . && docker run -p 8443:8443 -p 9000:9000 --rm -it keycloak

stop-keycloak:
	docker compose -f keycloak/docker-compose.yml down


run-lakekeeper:
	cd lakekeeper && docker build -t lakekeeper . && docker run --rm -it lakekeeper 

stop-lakekeeper:
	docker compose -f lakekeeper/docker-compose.yml down
