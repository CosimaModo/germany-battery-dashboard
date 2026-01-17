# Daten-Import Anleitung

## CSV-Datei aus dem Marktstammdatenregister exportieren

1. Öffne die MaStR-Webseite mit dem vorbereiteten Filter:
   https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht

2. Setze die Filter für:
   - Betriebs-Status: "In Betrieb" und "In Planung"
   - Speichertechnologie: "Lithium-Ionen" (Code 524)
   - Nettonennleistung der Einheit: > 999 kW (= >1 MW)
   - Name des Anlagenbetreibers: nicht "natürliche Person"

3. Klicke auf "CSV-Export" unter "Tabelle exportieren"

4. Speichere die Datei als `mastr_batteriespeicher.csv` in diesem Ordner

## Erwartete Spalten

Die CSV-Datei sollte folgende Spalten enthalten (deutsche MaStR-Namen):

| Spalte | Beschreibung |
|--------|--------------|
| MaStR-Nr. der Einheit | Eindeutige ID |
| Anzeigename der Einheit | Projektname |
| Betriebs-Status | "In Betrieb" oder "In Planung" |
| Nettonennleistung der Einheit | Leistung in kW |
| Nutzbare Speicherkapazität der Einheit | Kapazität in kWh |
| Inbetriebnahmedatum der Einheit | Datum (TT.MM.JJJJ) |
| Geplantes Inbetriebnahmedatum | Für Projekte in Planung |
| Name des Anlagenbetreibers (nur Org.) | Betreibername |
| Standort: Bundesland | Bundesland |

## Hinweise

- Die CSV-Datei verwendet Semikolon (;) als Trennzeichen
- Dezimalzahlen verwenden Komma (,) als Dezimaltrennzeichen
- Encoding: UTF-8

## Ohne CSV-Datei

Wenn keine CSV-Datei vorhanden ist, zeigt das Dashboard Beispieldaten an.
