# pitreos: Point in Time recovery for EOS
_Pronounced like "Patriots"_

# How to try out this wonderful copy/paste from stackoverflow:

1. Create a sparse file: truncate -s 512M file.img
2. run "go run splitfile.go"



# Proposal / discussion

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
 	on peut truncate si total_size <
        go lib: tar reader : cool pour sparse


## unknowns / risks:
  * When running the backup: what block are we on ? (race condition, cannot query when stopped...)
  * persistent volume claims bonne façon de faire ça ? attacher d'une autre, remanipuler, détruire juste le disque pas tout le stateful set... limitations kubernetes :(
  * sha1sum collision ? possible ?

## naming:
  * path doit contenir: "standard backup" vs tally ou bien history plugin backup... car plusieurs sortes sont possibles dépendamment des plugins

* Blobs are grouped by blob type
  note: we separate by file type to make it easier to change backup approach on one of them afterwards
  * state: 
   ```
   gs://eoscanada-playground-pitr/backups/state/blobs/{sha1sum.blob}
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.meta.yaml
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/forkdb.dat.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.meta.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/forkdb.dat.yaml
   ```
  * blobs:
   ``` 
   gs://eoscanada-playground-pitr/backups/blocks/blobs/{sha1sum.blob}
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.index.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.log.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.meta.yaml
   ```
   
## Example YAML file
```yaml
gs://eoscanada-playground-pitr/backups/state/1008385183/shared_memory.bin.yaml
---
file: state/shared_memory.bin
date: 2001-12-14t21:59:43.10-05:00
blobs_location: /backups/state/blobs
total_size: 150M
chunks:
- start: 0
  end: 50M
  content: 8483f4e21605df5af056e04a92face8823108135
- start: 50M
  end: 100M
  content: 0599afc9989c83feea29bd249c0910af9352706b
- start: 100M
  end: 150M
  content: null
```

