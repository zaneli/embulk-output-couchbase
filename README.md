# Couchbase output plugin for Embulk

[![Build Status](https://api.travis-ci.org/zaneli/embulk-output-couchbase.png?branch=master)](https://travis-ci.org/zaneli/embulk-output-couchbase)

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **host**: Couchbase Server host. If not provided, connect to localhost. (string, optional)
- **bucket**: bucket name. If not provided, open the default bucket. (string, optional)
- **password**: bucket password. If not provided, open the bucket without password. (string, optional)
- **id_column**: column name to be used as id. (string, required)
- **id_format**: id value format. It must contains `{id}` placeholder. If not provided, using only id column value itself. (string, optional)
- **write_mode**: `insert` or `upsert`. (string, default: `insert`)

## Example

```yaml
out:
  type: couchbase
  host: 192.168.111.22
  bucket: embulk_bucket
  id_column: id
  id_format: embulk_{id}
  write_mode: upsert
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Test

Start Couchbase Server on the localhost and prepare 'embulk_spec' bucket without password,  
or modify src/test/resources/env.conf file for adjust your environment.

```
$ ./gradlew test
```
