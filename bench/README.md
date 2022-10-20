# benchmark

Load testing with Benchmarks and possibility to compare results with old metrix.

## install

Build binary `bench` :

```bash
$ go build
```

Install binary `benchstat` :

```bash
$ go install golang.org/x/perf/cmd/benchstat@latest
```

## execution

Help :

```bash
$ bench -?
Usage: bench [-?hv] [-c config file name] [-o result directory output] [parameters ...]
 -?, --help                      Show command usage
 -c, --config=config             File name
 -f, --format=(html|log|stdout)  Output format
 -o, --output=result             Directory output
 -v, --version                   Output the version information
```

Run :

```bash
$ ./bench
```

Run with display on console :

```bash
$ ./bench -f stdout
```

## results

The results can be deposited in the `report` directory with a file nomenclature: `bench-20221023-124559.(log|html)`

The compare content :

```bash
name                  old time/op    new time/op    delta
GetCollectionItem-12    77.1µs ± 0%    88.4µs ± 0%   +14.70%

name                  old alloc/op   new alloc/op   delta
GetCollectionItem-12    7.31kB ± 0%   18.86kB ± 0%  +158.06%

name                  old allocs/op  new allocs/op  delta
GetCollectionItem-12       114 ± 0%       214 ± 0%   +87.72%

```

The benchmark result :

```bash
goos: linux
goarch: amd64
pkg: github.com/CrunchyData/pg_featureserv/internal/service/benchmarks
cpu: Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz
BenchmarkGetCollectionItem-12    	    7900	    133939 ns/op	    7268 B/op	     114 allocs/op
PASS
ok  	github.com/CrunchyData/pg_featureserv/internal/service/benchmarks	1.387s
```

## config

```json
{
    "cmd" : "benchstat",
    "host" : "localhost",
    "port" : 9000,
    "reportdir" : "report",
    "refdir" : "ref",
    "env" : [
        "DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pg_featureserv"
    ],
    "benchmarks" : [
        {
            "name" : "BenchmarkGetCollectionItem",
            "pkg" : "github.com/CrunchyData/pg_featureserv/internal/service/benchmarks",
            "ref" : "BenchmarkGetCollectionItem.ref",
            "actif" : 1,
            "env" : [
                "PGFS_CACHE=0"
            ]
        }
    ]
}
```

- `cmd` : statistics program name
- `env` : set global environment variables
- `benchmarks.name` : benchmark function name
- `benchmark.pkg` : benchmark package name
- `benchmark.ref` : ref benchmark to compare with the current
- `benchmark.env` : set environment variables for the current benchamark

**Note:**
> You could create reference of benchmark if you want to compare it. If the reference is empty, just the benchmark is executed.
