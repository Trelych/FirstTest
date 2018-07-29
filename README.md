#Client and server parts

Install and use


1. Make postgres database named "weather" in localhost 
login - "postgresql"
password - "asecurepassword"

Easyest way is to use 3 docker commands:
docker create -v /var/lib/postgresql/data --name postgres9.4-data busybox
docker run --name local-postgres9.4 -p 5432:5432 -e POSTGRES_PASSWORD=asecurepassword -d --volumes-from postgres9.4-data postgres:9.4
docker run -it --link local-postgres9.4:postgres --rm postgres:9.4 sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U postgres'

make database and tables using file Database.sql

if any parameters (address, user, pass) changed - correct it in first usage of sql.Open function(first calling function in main())

2. Compile or run server
  - go run server1.go
  - go build server1.go && ./server1.go
  
3. Compile or run server
  - go run client.go
  - go build client.go && ./client.go   



