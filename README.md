# pitreos: Point in Time Recovery Tool by EOS Canada
_Pronounced like "Patriots"_

# How to use ?

## compile and help
cd pitreos-cli && go install -v
pitreos-cli --help

## backup from nodeos

/pitreos-cli --bucket-name my-super-nodeos  --local-folder=nodeos-data --bucket-folder=pitreos --backup-tag=linux_ubuntu1604_gcc4_nohistory backup

## restore with given timestamp

/pitreos-cli --bucket-name my-super-nodeos  --local-folder=nodeos-data --bucket-folder=pitreos --backup-tag=linux_ubuntu1604_gcc4_nohistory --before-timestamp=$(date -d 'yesterday' +%s)" restore

# Optimizations TODO

make SHA1 computing parallel

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
   gs://eoscanada-playground-pitr/backups/state/blobs/{sha1sum}.blob
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.meta.yaml
   gs://eoscanada-playground-pitr/backups/state/standard/1008385183/forkdb.dat.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.meta.yaml
   gs://eoscanada-playground-pitr/backups/state/history/1008385183/forkdb.dat.yaml
   ```
  * blobs:
   ``` 
   gs://eoscanada-playground-pitr/backups/blocks/blobs/{sha1sum}.blob
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.index.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.log.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.bin.yaml
   gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.meta.yaml
   ```
  * conditions:
     {bucket-dir}: `backups`
     {data-type}: `state` or `blocks` # calculated for each defined folder
     {filename}: `reversible/shared_memory.bin` or `blocks.log` # calculated from "os.Walk"
     {backup-type}: `standard` ou `history`? ... # comes from "state-backup-type" and "blocks-backup-type"
     {bucket-name}: `eoscanada-playground-pitr`
     {timestamp}: `12345667778` # calculated from time.Now().Unix()

    blob goes to:
    `gs://{bucket-name}/{dest-prefix}/{data-type}/blobs/{sha1sum}.blob`
    yaml metadata goes to
    `gs://{bucket-name}/{dest-prefix}/{data-type}/{backup-type}/{timestamp}/{filename}.yaml`

    #example with all default values !
    ./pitreos backup --bucket-dir=backups --state-folder=state --blocks-folder=blocks --bucket-name=eoscanada-playground-pitr --state-backup-type=standard --blocks-backup-type=standard
    ./pitreos restore --bucket-dir=backups  --state-folder=state --blocks-folder=blocks --bucket-name=eoscanada-playground-pitr --state-backup-type=standard --blocks-backup-type=standard --timestamp=1008385183

    #note: we could have a "recent.yaml" file to which we append the "recent" ( 3months ???) successful backups which we can serve for different backup-types
    - timestamp: 1235477665
      blocks_backup_types:
      - name: standard
      state_backup_types:
      - name: standard
      - name: history
      metadata_files: 
      - gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.index.yaml
      - gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/blocks.log.yaml
      - gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.bin.yaml
      - gs://eoscanada-playground-pitr/backups/blocks/standard/1008385183/reversible/shared_memory.meta.yaml
      - gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.bin.yaml
      - gs://eoscanada-playground-pitr/backups/state/standard/1008385183/shared_memory.meta.yaml
      - gs://eoscanada-playground-pitr/backups/state/standard/1008385183/forkdb.dat.yaml
      - gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.bin.yaml
      - gs://eoscanada-playground-pitr/backups/state/history/1008385183/shared_memory.meta.yaml
      - gs://eoscanada-playground-pitr/backups/state/history/1008385183/forkdb.dat.yaml


      
   
## Example YAML file
```yaml
gs://eoscanada-playground-pitr/backups/state/1008385183/shared_memory.bin.yaml
---
metaversion: 1                       # check this before going forward
fileName: state/shared_memory.bin    # restore to that point
date: 2001-12-14t21:59:43.10-05:00
blobsLocation: backups/state/blobs
totalSize: 150M
totalChunks
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

