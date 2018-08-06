# pitreos: Point in Time recovery for EOS
_Pronounced like "Patriots"_


## Use cases for PIT recovery 

(2h granularité is enough)

* surtout replay partiel
* start dev machin test
* exploratoire (replay ...)
* replce dead node
* ajoute node 
* 30 minutes va grossir ... -> plus !
* diminuer bandwidth

## comment ?
* client: (eosc/manageos) 
* pas besoin serveur: static storage -> api public $$$
* 2h + catchup de nodeos via un chainfreezer -> out of our scope!!

types de données:
* bindiff + metadata des fichiers
  * state
  * blocks/reversible

  * blocklog: incrémental only
 	on peut truncate si total_size
        go lib: tar reader : cool pour sparse


## unknowns:
  * quel bloc on est ? (race condition) -> log donné par manageos
  * persistent volume claims bonne façon de faire ça ? attacher d'une autre, remanipuler, détruire juste le disque pas tout le stateful set... limitations kubernetes :(

## naming:
  * path doit contenir: "standard backup" vs tally ou bien history plugin backup... car plusieurs sortes sont possibles dépendamment des plugins

# Exemple de fichier
```yaml
gs://gnagna/backups/2018-08-01-01-01-01/state/shared_memory.bin.meta
---
file: state/shared_memory.bin
total_size: ...
chunks:
- start: 0
  end: 50M
  content: ABCDF1231231231231ABCDF123123ACBD
- start: 50000001
  end: 100000000
  content: ABCDEF123123123128392389182391829389
- start: 150M
  end: 200M
  content: null
---

gs://gnagna/backups_contents/ABCDEF12312312321ABCDF123123ABCD
gs://gnagna/backups_contents/ABCDEF123123123123812378921749821398

file: blocks/block.log
metachunkcs:
- start: 0
```




