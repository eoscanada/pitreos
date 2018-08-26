# pitreos: Point in Time Recovery Tool by EOS Canada
_Pronounced like "Patriots"_

Backup and restore tool optimized for large files that don't change much
Perfect for EOS state and block.log, or virtual machine images

<p align="center">
    <img src="https://eoscanada.github.io/terminal/pitreos_term.svg">
</p>

# Supported platforms

* Linux
* OSX (no sparse file optimization)

# How does it work ?
## Backing up
1. It splits each file into smaller chunks and computes the hashes of each of those.
2. It sends those chunks along with a YAML metadata file linking to the file chunks.


## Restoring
1. Restore process first looks at the remote location for "index.yaml" files under timestamp-named folders for the most recent backup before the requested timestamp.
2. The chosen index.yaml lists all the files to restore, with URLs to their metadata yaml files 
3. Each non-existing local file is created as an empty sparse file with the expected length
4. Existing files are truncated (or enlarged) to the expected length
5. Local chunks are sha1sum'd and compared to the expected chunk

## Optimizations:
* Empty chunks (no data or only null bytes) are not transferred
* Unassigned chunks in sparse files are not even read or written to
* Chunks with data are compressed before transfer
* Caching can be enabled to keep any downloaded/uploaded chunk locally and quickly restore your files.
* Chunks are not uploaded again if the same content exist at the same destination (with the same backup path)

# How to install ?

1. Ensure that you have a sane GOLANG environment, with your PATH to $GOPATH/bin
2. Run the following commands from the repo
```
$ make deps
$ make install
```

# Example uses

## Backup to Google Storage

`pitreos -c backup --metadata '{"blocknum": 123456, "version": "1.2.1"}' . gs://mybackups/projectname `
 --> This will send your data chunks under `gs://mybackups/projectname/blobs/`
 --> your metadata files will be located under `gs://mybackups/projectname/{timestamp}/`
 --> The backup metadata will contain the provided arbitrary values "blocknum" and "version"

## Restore with given timestamp

`pitreos -c restore gs://mybackups/projectname . --timestamp $(date -d "10 minutes ago" +%s)`
 --> This will restore your data using the most recent backup *before* 10 minutes ago, based on the timestamps found under `gs://mybackups/projectname/{timestamp}`

## More examples ##

Run "pitreos help", "pitreos help backup" and "pitreos help restore" for more examples

