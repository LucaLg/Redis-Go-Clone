CodeCrafters Redis Challenge in Go
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

Durch lösen von verschiedener Aufgaben, wird schrittweise ein Redis-Server in Go implementiert.
Dabei wird jeweils nur die gewünschte Funktionalität erklärt und ein Testcase vorgegeben.
Die Implementierung wird dem Entwickler überlassen. Nach lösen einer Aufgabe wird der Code automatisch getestet . Falls die Tests erfolgreich sind, wird die nächste Aufgabe freigeschaltet.

Folgende Redis spezifische oder Go spezifische Funktionalitäten werden in den Aufgaben behandelt:

- Implementierung von Redis-Befehlen wie GET SET DEL
- TCP-Server in Go mit Read- und Write-Operationen, Connection Handling und Error Handling
- Redis Bulk Strings Parser
- Redis RDB (Redis Database) Parser [RDB Format](https://rdb.fnordig.de/file_format.html)
- Redis Replication Server mit Handshake und Datenübertragung
- Redis Stream Format mit XADD und XREAD Befehlen die Streams lesen und schreiben

## Starten des Redis-Servers

```bash
./spawn_redis_server.sh
```

mit Port flag

```bash
./spawn_redis_server.sh -port 6379
```

als Replication Server verbunden mit dem Master Server auf localhost:6379

```bash
./spawn_redis_server.sh --port 6380 --replicaof localhost 6379
```

für weitere Optionen muss die redis-cli installiert sein und kann mit `redis-cli --help` aufgerufen werden.
Beispiel:

```bash
redis-cli
redis-cli set foo bar
redis-cli get foo
```
