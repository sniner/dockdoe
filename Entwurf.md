---
created: 2026-06-06T15:09:49+02:00
updated: 2026-06-06T15:36:52+02:00
title: Projekt DockDoe
aliases: [Projekt DockDoe]
linter-yaml-title-alias: Projekt DockDoe
---

# Projekt DockDoe

Ziel: Ein Single Binary auf dem Docker Host, der eine WebUI darstellt, in der man die vitalen Metriken der Container sehen kann.

- die Messwerte werden von der Server App aufgenommen und in einer noch zu definierenden Datenhaltung aufbewahrt.
- Bis zum Zeitpunkt A alle Werte, parallel werden daraus Trendwerte berechnet (wie Zabbix): min, max, median. Entweder pro Zeitfenster oder X Werte, je nachdem was sinnvoller ist. Die Trendwerte werden immer sofort berechnet, wenn Zeitfenster oder X Werte erreicht sind, nicht erst dann, wenn sie aus dem Zeitpunkt A rauswandern. Hat den Vorteil, dass die Logik einfach bleibt und man A relativ stressfrei anpassen kann. Falls ein Trendwert für den aktuellen Zeitpunkt benötigt wird: einfach gewichten mit der Anzahl Werte über Anzahl für komplettes Fenster. Median oder Mittelwert? Ich bevorzuge Median, der ist robuster gegen Spikes und man hat ja noch den max Wert.
- Optisch darf es sich an Dockge orientieren. Aber nicht von der Funktionalität, denn wenn ich Dockge gut fände, würde ich nicht über eine eigene Lösung nachdenken.
- Die WebUI wird aus statischem CSS und HTML im Binary gespeist. Beim ersten Aufruf werden die kompletten Werte für das Anzeigeintervall übertragen, danach die neuen Werte gestreamt.
- Das WebUI soll responsive sein und interaktiv. Ob dafür JS nötig ist oder HTMX im Backend reicht, kann ich nicht beurteilen.
- Was will man sehen:
    - Ein Dashboard:
        - Alle Container auf einen Blick (ggf. geklammert pro Stack/Compose) mit Zustand (up, down, healthy, error) und Memorybedarf und CPU-Last
        - Im Kopfbereich ein paar Werte die für den ganzen Host sind: CPU Last + Load 1/5/10 + Memory used/free, wie halt bei htop.
    - Detailseite pro Stack:
        - im Kopfbereich die Leistungsdaten: CPU, Memory als kleine Graphen + Action Buttons für Start/Stop/Restart
        - Das kann über Tabs gelöst werden: Im unteren Bereich die Log-Ausgaben, aktuell. Nächster Tab: Die compose.yml (irgendwann ggf. mal mit Editor)
        - Irgendwo braucht es auch einen Bereich, der für einen einzelnen Container da ist. Auch dort die Leistungsdaten und die Action Buttons + weitere Angaben wie Containername etc.
- Angeblich liefert /run/docker.sock keine CPU-Werte und angeblich gäbe es da Fallstricke:

  > **Der eine echte Fallstrick** ist die CPU-Prozentberechnung. Die Docker-Engine-API (`/containers/{id}/stats`) liefert dir Memory, Net-I/O und Block-I/O mehr oder weniger direkt – aber **nicht** CPU%. Das musst du selbst aus Deltas rechnen: `cpu_delta / system_delta * online_cpus * 100`, also `cpu_stats` gegen `precpu_stats`. Genau da hauen naive Implementierungen daneben oder zeigen Unsinn. `docker stats` macht intern exakt das. Wenn du das sauber hinkriegst, bist du schon besser als die halbe Konkurrenz.

Trivia: Der Name wird im Projekt nicht erklärt, er ist einfach so. Gesucht hatte ich nach "Doc" + irgendwas cooles. Da mir nix einfiel, kam ich John Doe, daher "Doc Doe" oder auf Docker gemünzt. DockDoe.
