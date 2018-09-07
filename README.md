# pitreos: Point in Time Recovery Tool by EOS Canada
_Pronounced like "Patriots"_

Backup and restore tool optimized for large sparse files and append-only files.

Perfect for EOS.IO blockchains' `state` and `blocks` logs, virtual
machine images or other large files that change only in part.

<p align="center">
  <img src="https://eoscanada.github.io/terminal/pitreos_term.svg">
</p>

# Supported platforms

* Linux
* OSX (no sparse file optimization)

# How does it work ?
## Backing up
1. It splits each file into smaller chunks and computes the hashes of each of those.
2. It sends those chunks to a defined storage URL along with an index file linking to the file chunks.
3. Those chunks can also be cached locally.
4. Backups are stored with a timestamp and an optional tag. Use tags to differentiate backups which may share data chunks but have different uses (ex: dev vs prod, osx vs linux...)

## Restoring
1. The "list" command will fetch the last backups from your defined storage URL
2. The "restore" command will fetch the backup index chosen chosen from the list. (Alternatively, you can ask pitreos to restore to the latest backup of a specific tag)
3. The backup index is parsed: it contains the list of files and their content based on their chunk hashes.
4. Each non-existing local file is created as an empty sparse file with the expected length, while existing files are truncated (or enlarged) to the expected length
5. Local chunks are hashed and compared to the expected chunk.
6. Chunks which should be empty get a hole punched through them (becoming a sparse file)
7. Chunks which should have different content are downloaded from your backup store.

## Optimizations:
* Empty chunks (no data or only null bytes) are not transferred
* Unassigned chunks in sparse files are not even read or written to
* Chunks with data are compressed before transfer
* Caching can be enabled to keep any downloaded/uploaded chunk locally and quickly restore your files.
* Chunks are not uploaded again if the same content exist at the same destination (with the same backup path)
* Existing data in files flagged as "appendonly-files" are not verified on restore. Only missing data at the end of the file is downloaded.

## Known issues:
* File permissions are not managed

# How to install ?

1. Ensure that you have a sane GOLANG environment, with your PATH to $GOPATH/bin
2. Run the following commands from the repo
```
$ go get ./...
$ go install -v
```

# Examples

## Example .pitreos.yaml in your workspace

```# $HOME/myproject/.pitreos.yaml
store: gs://mybackups/nodeos_data
tag: john_dev
```

## Backup to default location

```pitreos backup ./mydata```
* This will send your chunks to $HOME/.pitreos/backups/chunks/{sha3_256sum}
* This will send your backup index to $HOME/.pitreos/backups/indexes/{timestamp}-default.yaml.gz

## Backup to Google Storage

```pitreos backup mydata -s gs://mybackups/projectname -t dev -c -m '{"blocknum": 123456, "version": "1.2.1"}'```
* This will send your data chunks under `gs://mybackups/projectname/chunks/`
* your backup index file will be located under `gs://mybackups/projectname/indexes/{timestamp}-dev.yaml.gz`
* your chunks file will also be savec in your default cache location ($HOME/.pitreos/cache)
* The backup metadata will contain the provided arbitrary values "blocknum" and "version"

## List your 5 last backups
```pitreos list --limit 5```
* 2018-08-28-19-24-59--default
* 2018-08-28-18-15-39--john-dev

## Restore a specific backup
```pitreos -c restore 2018-08-28-18-15-48--john-dev ./mydata```
* This will restore your data from that backup under ./mydata. Any file already existing there may speed up the process.

## Restore from latest backup of a tag
```pitreos -c restore -t john-dev ./mydata```
* This will restore your data from the latest backup with the "john-dev" tag.

## More examples in help !
Run "pitreos help", "pitreos help backup" and "pitreos help restore" for more examples
