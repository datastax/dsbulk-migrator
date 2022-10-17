# DSBulk Migrator

A tool to migrate tables between two clusters, leveraging the DataStax Bulk Loader (DSBulk) to
perform the actual data migration.

The tool provides the following main commands:

* `migrate-live` starts a live data migration using a pre-existing DSBulk installation, or
  alternatively, the embedded DSBulk version. A "live" migration means that the data migration will
  start immediately and will be performed by this migrator tool through the desired DSBulk
  installation.

* `generate-script` generates a migration script that, once executed, will perform the desired data
  migration, using a pre-existing DSBulk installation. Please note: this command does not actually
  migrate the data; it only generates the migration script.

* `generate-ddl` reads the schema from the origin cluster and generates CQL files to recreate it in
  the target cluster.

## Building

Build is done with Maven:

    mvn clean package

The build produces two distributable fat jars:

* `dsbulk-migrator-<VERSION>-embedded-driver.jar` : contains an embedded Java driver; suitable for
  live migrations using an external DSBulk, or for script generation. This jar is NOT suitable for
  live migrations using an embedded DSBulk, since no DSBulk classes are present.

* `dsbulk-migrator-<VERSION>-embedded-dsbulk.jar`: contains an embedded DSBulk and an embedded Java
  driver; suitable for all operations. Note that this jar is much bigger than the previous one, due
  to the presence of DSBulk classes.

## Testing

The project contains a few integration tests. Run them with:

    mvn clean verify

The integration tests require [Simulacron](https://github.com/datastax/simulacron). Be sure to meet
all the [prerequisites](https://github.com/datastax/simulacron#prerequisites) before running the
tests.

## Running

Launch the tool as follows:

    java -jar /path/to/dsbulk-migrator.jar (migrate-live|generate-script|generate-ddl) [OPTIONS]

When doing a live migration, the options are used to effectively configure DSBulk and to connect to
the clusters.

When generating a migration script, most options serve as default values in the generated scripts.
Note however that, even when generating scripts, this tool still needs to access the origin cluster
in order to gather metadata about the tables to migrate.

When generating a DDL file, only a few options are meaningful. Since DSBulk is not used, and the
import cluster is never contacted, import options and DSBulk-related options are ignored. The tool
still needs to access the origin cluster in order to gather metadata about the keyspaces and tables
to generate DDL statements for.

### Live Migration Command Line Options

The following options are available for the `migrate-live` command; most have sensible default values and do not
need to be specified, unless you want to override the default value:

```
  -c, --dsbulk-cmd=CMD       The external DSBulk command to use. Ignored if the embedded DSBulk is
                               being used. The default is simply 'dsbulk', assuming that the
                               command is available through the PATH variable contents.
  -d, --data-dir=PATH        The directory where data will be exported to and imported from.The
                               default is a 'data' subdirectory in the current working directory.
                               The data directory will be created if it does not exist. Tables will
                               be exported and imported in subdirectories of the data directory
                               specified here; there will be one subdirectory per keyspace inside
                               the data directory, then one subdirectory per table inside each
                               keyspace directory.
  -e, --dsbulk-use-embedded  Use the embedded DSBulk version instead of an external one. The
                               default is to use an external DSBulk command.
      --export-bundle=PATH   The path to a secure connect bundle to connect to the origin cluster,
                               if that cluster is a DataStax Astra cluster. Options --export-host
                               and --export-bundle are mutually exclusive.
      --export-consistency=CONSISTENCY
                             The consistency level to use when exporting data. The default is
                               LOCAL_QUORUM.
      --export-dsbulk-option=OPT=VALUE
                             An extra DSBulk option to use when exporting. Any valid DSBulk option
                               can be specified here, and it will passed as is to the DSBulk
                               process. DSBulk options, including driver options, must be passed as
                               '--long.option.name=<value>'. Short options are not supported.
      --export-host=HOST[:PORT]
                             The host name or IP and, optionally, the port of a node from the
                               origin cluster. If the port is not specified, it will default to
                               9042. This option can be specified multiple times. Options
                               --export-host and --export-bundle are mutually exclusive.
      --export-max-concurrent-files=NUM|AUTO
                             The maximum number of concurrent files to write to. Must be a positive
                               number or the special value AUTO. The default is AUTO.
      --export-max-concurrent-queries=NUM|AUTO
                             The maximum number of concurrent queries to execute. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --export-max-records=NUM
                             The maximum number of records to export for each table. Must be a
                               positive number or -1. The default is -1 (export the entire table).
      --export-password      The password to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all. Omit the parameter value to be prompted for
                               the password interactively.
      --export-splits=NUM|NC The maximum number of token range queries to generate. Use the NC
                               syntax to specify a multiple of the number of available cores, e.g.
                               8C = 8 times the number of available cores. The default is 8C. This
                               is an advanced setting; you should rarely need to modify the default
                               value.
      --export-username=STRING
                             The username to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all.
  -h, --help                 Displays this help message.
      --import-bundle=PATH   The path to a secure connect bundle to connect to the target cluster,
                               if that cluster is a DataStax Astra cluster. Options --export-host
                               and --export-bundle are mutually exclusive.
      --import-consistency=CONSISTENCY
                             The consistency level to use when importing data. The default is
                               LOCAL_QUORUM.
      --import-default-timestamp=<defaultTimestamp>
                             The default timestamp to use when importing data. Must be a valid
                               instant in ISO-8601 syntax. The default is 1970-01-01T00:00:00Z.
      --import-dsbulk-option=OPT=VALUE
                             An extra DSBulk option to use when importing. Any valid DSBulk option
                               can be specified here, and it will passed as is to the DSBulk
                               process. DSBulk options, including driver options, must be passed as
                               '--long.option.name=<value>'. Short options are not supported.
      --import-host=HOST[:PORT]
                             The host name or IP and, optionally, the port of a node from the
                               target cluster. If the port is not specified, it will default to
                               9042. This option can be specified multiple times. Options
                               --export-host and --export-bundle are mutually exclusive.
      --import-max-concurrent-files=NUM|AUTO
                             The maximum number of concurrent files to read from. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --import-max-concurrent-queries=NUM|AUTO
                             The maximum number of concurrent queries to execute. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --import-max-errors=NUM
                             The maximum number of failed records to tolerate when importing data.
                               The default is 1000. Failed records will appear in a load.bad file
                               inside the DSBulk operation directory.
      --import-password      The password to use to authenticate against the target cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all. Omit the parameter value to be prompted for
                               the password interactively.
      --import-username=STRING
                             The username to use to authenticate against the target cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all.
  -k, --keyspaces=REGEX      A regular expression to select keyspaces to migrate. The default is to
                               migrate all keyspaces except system keyspaces, DSE-specific
                               keyspaces, and the OpsCenter keyspace. Case-sensitive keyspace names
                               must be entered in their exact case.
  -l, --dsbulk-log-dir=PATH  The directory where DSBulk should store its logs. The default is a
                               'logs' subdirectory in the current working directory. This
                               subdirectory will be created if it does not exist. Each DSBulk
                               operation will create a subdirectory inside the log directory
                               specified here.
      --max-concurrent-ops=NUM
                             The maximum number of concurrent operations (exports and imports) to
                               carry. Default is 1. Set this to higher values to allow exports and
                               imports to occur concurrently; e.g. with a value of 2, each table
                               will be imported as soon as it is exported, while the next table is
                               being exported.
      --skip-truncate-confirmation
                             Skip truncate confirmation before actually truncating tables. Only
                               applicable when migrating counter tables, ignored otherwise.
  -t, --tables=REGEX         A regular expression to select tables to migrate.The default is to
                               migrate all tables inside the keyspaces that were selected for
                               migration with --keyspaces. Case-sensitive table names must be
                               entered in their exact case.
      --table-types=regular|counter|all
                             The table types to migrate (regular, counter, or all). Default is all.
      --truncate-before-export
                             Truncate tables before the export instead of after. Default is to
                               truncate after the export. Only applicable when migrating counter
                               tables, ignored otherwise.
  -w, --dsbulk-working-dir=PATH
                             The directory where DSBulk should be executed. Ignored if the embedded
                               DSBulk is being used. If unspecified, it defaults to the current
                               working directory.
```

### Script Generation Command Line Options

The following options are available for the `generate-script` command; most have sensible default values and do not
need to be specified, unless you want to override the default value:

```
  -c, --dsbulk-cmd=CMD       The DSBulk command to use. The default is simply 'dsbulk', assuming
                               that the command is available through the PATH variable contents.
  -d, --data-dir=PATH        The directory where migration scripts will be generated. The default
                               is a 'data' subdirectory in the current working directory. The data
                               directory will be created if it does not exist.
      --export-bundle=PATH   The path to a secure connect bundle to connect to the origin cluster,
                               if that cluster is a DataStax Astra cluster. Options --export-host
                               and --export-bundle are mutually exclusive.
      --export-consistency=CONSISTENCY
                             The consistency level to use when exporting data. The default is
                               LOCAL_QUORUM.
      --export-dsbulk-option=OPT=VALUE
                             An extra DSBulk option to use when exporting. Any valid DSBulk option
                               can be specified here, and it will passed as is to the DSBulk
                               process. DSBulk options, including driver options, must be passed as
                               '--long.option.name=<value>'. Short options are not supported.
      --export-host=HOST[:PORT]
                             The host name or IP and, optionally, the port of a node from the
                               origin cluster. If the port is not specified, it will default to
                               9042. This option can be specified multiple times. Options
                               --export-host and --export-bundle are mutually exclusive.
      --export-max-concurrent-files=NUM|AUTO
                             The maximum number of concurrent files to write to. Must be a positive
                               number or the special value AUTO. The default is AUTO.
      --export-max-concurrent-queries=NUM|AUTO
                             The maximum number of concurrent queries to execute. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --export-max-records=NUM
                             The maximum number of records to export for each table. Must be a
                               positive number or -1. The default is -1 (export the entire table).
      --export-password      The password to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all. Omit the parameter value to be prompted for
                               the password interactively.
      --export-splits=NUM|NC The maximum number of token range queries to generate. Use the NC
                               syntax to specify a multiple of the number of available cores, e.g.
                               8C = 8 times the number of available cores. The default is 8C. This
                               is an advanced setting; you should rarely need to modify the default
                               value.
      --export-username=STRING
                             The username to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all.
  -h, --help                 Displays this help message.
      --import-bundle=PATH   The path to a secure connect bundle to connect to the target cluster,
                               if that cluster is a DataStax Astra cluster. Options --export-host
                               and --export-bundle are mutually exclusive.
      --import-consistency=CONSISTENCY
                             The consistency level to use when importing data. The default is
                               LOCAL_QUORUM.
      --import-default-timestamp=<defaultTimestamp>
                             The default timestamp to use when importing data. Must be a valid
                               instant in ISO-8601 syntax. The default is 1970-01-01T00:00:00Z.
      --import-dsbulk-option=OPT=VALUE
                             An extra DSBulk option to use when importing. Any valid DSBulk option
                               can be specified here, and it will passed as is to the DSBulk
                               process. DSBulk options, including driver options, must be passed as
                               '--long.option.name=<value>'. Short options are not supported.
      --import-host=HOST[:PORT]
                             The host name or IP and, optionally, the port of a node from the
                               target cluster. If the port is not specified, it will default to
                               9042. This option can be specified multiple times. Options
                               --export-host and --export-bundle are mutually exclusive.
      --import-max-concurrent-files=NUM|AUTO
                             The maximum number of concurrent files to read from. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --import-max-concurrent-queries=NUM|AUTO
                             The maximum number of concurrent queries to execute. Must be a
                               positive number or the special value AUTO. The default is AUTO.
      --import-max-errors=NUM
                             The maximum number of failed records to tolerate when importing data.
                               The default is 1000. Failed records will appear in a load.bad file
                               inside the DSBulk operation directory.
      --import-password      The password to use to authenticate against the target cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all. Omit the parameter value to be prompted for
                               the password interactively.
      --import-username=STRING
                             The username to use to authenticate against the target cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all.
  -k, --keyspaces=REGEX      A regular expression to select keyspaces to migrate. The default is to
                               migrate all keyspaces except system keyspaces, DSE-specific
                               keyspaces, and the OpsCenter keyspace. Case-sensitive keyspace names
                               must be entered in their exact case.
  -l, --dsbulk-log-dir=PATH  The directory where DSBulk should store its logs. The default is a
                               'logs' subdirectory in the current working directory. This
                               subdirectory will be created if it does not exist. Each DSBulk
                               operation will create a subdirectory inside the log directory
                               specified here.
  -t, --tables=REGEX         A regular expression to select tables to migrate.The default is to
                               migrate all tables inside the keyspaces that were selected for
                               migration with --keyspaces. Case-sensitive table names must be
                               entered in their exact case.
      --table-types=regular|counter|all
                             The table types to migrate (regular, counter, or all). Default is all.
```

### DDL Generation Command Line Options

The following options are available for the `generate-ddl` command; most have sensible default values and do not
need to be specified, unless you want to override the default value:

```
  -a, --optimize-for-astra   Produce CQL scripts optimized for DataStax Astra. Astra does not allow
                               some options in DDL statements; using this option, forbidden options
                               will be omitted from the generated CQL files.
  -d, --data-dir=PATH        The directory where CQL files will be generated. The default is a
                               'data' subdirectory in the current working directory. The data
                               directory will be created if it does not exist.
      --export-bundle=PATH   The path to a secure connect bundle to connect to the origin cluster,
                               if that cluster is a DataStax Astra cluster. Options --export-host
                               and --export-bundle are mutually exclusive.
      --export-host=HOST[:PORT]
                             The host name or IP and, optionally, the port of a node from the
                               origin cluster. If the port is not specified, it will default to
                               9042. This option can be specified multiple times. Options
                               --export-host and --export-bundle are mutually exclusive.
      --export-password      The password to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all. Omit the parameter value to be prompted for
                               the password interactively.
      --export-username=STRING
                             The username to use to authenticate against the origin cluster.
                               Options --export-username and --export-password must be provided
                               together, or not at all.
  -h, --help                 Displays this help message.
  -k, --keyspaces=REGEX      A regular expression to select keyspaces to export. The default is to
                               export all keyspaces except system keyspaces, DSE-specific
                               keyspaces, and the OpsCenter keyspace. Case-sensitive keyspace names
                               must be entered in their exact case.
  -t, --tables=REGEX         A regular expression to select tables to export.The default is to
                               export all tables inside the keyspaces that were selected for export
                               with --keyspaces. Case-sensitive table names must be entered in
                               their exact case.
```
## Getting help

Global help is available as follows:

    java -jar /path/to/dsbulk-migrator-embedded-dsbulk.jar --help

This will print help about the available commands.

Per-command help is available as follows:

    java -jar /path/to/dsbulk-migrator-embedded-dsbulk.jar COMMAND --help

This will print detailed help about the selected command along with all the available options for
the specified command.

## Examples

Generate a migration script to migrate from an existing cluster to an Astra cluster:

    java -jar target/dsbulk-migrator-<VERSION>-embedded-driver.jar migrate-live \
        --data-dir=/path/to/data/dir \
        --dsbulk-cmd=${DSBULK_ROOT}/bin/dsbulk \
        --dsbulk-log-dir=/path/to/log/dir \
        --export-host=my-origin-cluster.com \
        --export-username=user1 \
        --export-password=s3cr3t \
        --import-bundle=/path/to/bundle \
        --import-username=user1 \
        --import-password=s3cr3t

Migrate live from an existing cluster to an Astra cluster using an external DSBulk installation;
passwords will be prompted interactively:

    java -jar target/dsbulk-migrator-<VERSION>-embedded-driver.jar migrate-live \
        --data-dir=/path/to/data/dir \
        --dsbulk-cmd=${DSBULK_ROOT}/bin/dsbulk \
        --dsbulk-log-dir=/path/to/log/dir \
        --export-host=my-origin-cluster.com \
        --export-username=user1 \
        --export-password # password will be prompted \
        --import-bundle=/path/to/bundle \
        --import-username=user1 \
        --import-password # password will be prompted

Migrate live from an existing cluster to an Astra cluster using the embedded DSBulk installation;
passwords will be prompted interactively; extra DSBulk options are passed:

    java -jar target/dsbulk-migrator-<VERSION>-embedded-dsbulk.jar migrate-live \
        --data-dir=/path/to/data/dir \
        --dsbulk-use-embedded \
        --dsbulk-log-dir=/path/to/log/dir \
        --export-host=my-origin-cluster.com \
        --export-username=user1 \
        --export-password # password will be prompted \
        --export-dsbulk-option "--connector.csv.maxCharsPerColumn=65536" \
        --export-dsbulk-option "--executor.maxPerSecond=1000" \
        --import-bundle=/path/to/bundle \
        --import-username=user1 \
        --import-password # password will be prompted \
        --import-dsbulk-option "--connector.csv.maxCharsPerColumn=65536" \
        --import-dsbulk-option "--executor.maxPerSecond=1000" 

Note that for the last example, you must use the `dsbulk-migrator-<VERSION>-embedded-dsbulk.jar` fat
jar, otherwise, an error will be raised because no embedded DSBulk can be found.

Generate DDL files to re-create the origin schema in an Astra target cluster:

    java -jar target/dsbulk-migrator-<VERSION>-embedded-driver.jar generate-ddl \
        --data-dir=/path/to/data/dir \
        --export-host=my-origin-cluster.com \
        --export-username=user1 \
        --export-password=s3cr3t \
        --optimize-for-astra

