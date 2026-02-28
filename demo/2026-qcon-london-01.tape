Output show.mp4

Set FontSize 30
Set Width 1920
Set Height 1080
Set Padding 30
Set Theme "Builtin Light"

# Set WindowBar Colorful
# Set WindowBarSize 40
# Set BorderRadius 8

Hide

Type "pkill tansu"
Enter

Type@1ms "rm -f tansu.db*"
Enter@1ms

Type@1ms "rm -rf lake/*"
Enter@1ms

Type@1ms "cargo build --bin tansu --no-default-features --features libsql,parquet"
Enter@1ms

Type@1ms "export RUST_LOG=warn"
Enter@1ms

Type@1ms "alias tansu=target/debug/tansu"
Enter@1ms

Ctrl+L
Show

Type "# employee protocol buffer schema"
Enter
Type "highlight etc/schema/employee.proto"
Enter
Enter
Sleep 6s

Type "# start broker with schema validation and parquet output"
Enter
Type "tansu broker \"
Enter
Type "--storage-engine=sqlite://tansu.db \"
Enter
Type "--schema-registry=file://./etc/schema \"
Enter
Type "parquet --location=file://./lake 2>&1 >broker.log &"
Enter
Enter
Sleep 1

Type "# create employee as a sink topic"
Enter
Type "tansu topic create employee --config=tansu.lake.sink=true"
Enter
Enter
Sleep 1


Type "# sample employee data"
Enter
Type "highlight etc/data/employees.json"
Enter
Enter
Sleep 2s

Type "# produce sample data using protobuf to employee topic"
Enter
Type "tansu cat produce employee etc/data/employees.json"
Enter
Enter
Sleep 5s

Type "# lake/{topic}/{partition}/{offset}.parquet"
Enter
Type "ls -l lake/employee/0000000000"
Enter
Enter
Sleep 1s

Type "# select produced parquet"
Enter
Type `duckdb -box :memory: "SELECT unnest(meta),unnest(key),unnest(value) FROM 'lake/employee/*/*.parquet'"`
Enter
Enter
Sleep 10s

Type "# No Kafka produced to SQLite"
Enter
Type `sqlite3 -box tansu.db "select count(*) from record"`
Enter
Enter
Sleep 5s

Type "# topic metadata has been updated in SQLite"
Enter
Type `sqlite3 -box tansu.db "select * from watermark"`
Enter
Sleep 5s

Type "# broker memory usage RSS MB"
Enter
Type `ps -p $(pgrep tansu) -o rss= | awk '{print $1/1024 " MB"}'`
Enter
Sleep 5s

Hide
Type "pkill tansu"
Enter

Ctrl+L
Show
