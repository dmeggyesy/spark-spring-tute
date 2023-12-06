### Quickstart


- `gradle shadowJar`
- `cd docker`
- `docker compose up -d`
- `cd ..`
- `gradle bootRun`

Gradle shadowJar will create the UberJar required to be passed through to the docker containers.  (localted in spark-spring-pub project).
