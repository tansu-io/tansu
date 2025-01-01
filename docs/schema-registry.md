# Schema Registry

Tansu has an embedded schema registry that supports [JSON schema][json-schema-org].
The schema registry is enabled using the `--schema-registry` command-line
parameter to the server. A schema registry is a `file` or `s3` URL containing
schemas that Tansu uses to validate messages.
When a message is produced to a topic with an associated schema,
the message is validated against the schema. If a message does not conform to its
schema it is rejected with an `INVALID_RECORD` error.

## JSON schema

An example JSON schema for the "person" topic:

```json
{
  "title": "Person",
  "type": "object",
  "properties": {

    "key": {
      "type": "string",
      "pattern": "^[A-Z]{3}-\\d{3}$"
    },

    "value": {
      "type": "object",
      "properties": {
        "firstName": {
          "type": "string",
          "description": "The person's first name."
        },
        "lastName": {
          "type": "string",
          "description": "The person's last name."
        },
        "age": {
          "description": "Age in years which must be equal to or greater than zero.",
          "type": "integer",
          "minimum": 0
        }
      }
    }
  }
}
```

The schema must be an object, with properties for the message "key" and/or "value".

The person schemas can be found in the `etc/schema` directory of the Tansu GitHub
repository. This directory is also used when starting Tansu using
the `just tansu-server` recipe or Docker compose.

Starting Tansu with schema validation enabled:

```shell
❯ just tansu-server
./target/debug/tansu-server --kafka-cluster-id ${CLUSTER_ID}
                            --kafka-advertised-listener-url tcp://${ADVERTISED_LISTENER}
                            --schema-registry file://./etc/schemas
                            --storage-engine ${STORAGE_ENGINE} 2>&1 | tee tansu.log
```

Create the person topic:

```shell
❯ just person-topic-create
kafka-topics --bootstrap-server localhost:9092
             --partitions=3
             --replication-factor=1
             --create
             --topic person
Created topic person.
```

Produce a message that is valid for the person schema:

```shell
❯ just person-topic-produce-valid
echo 'h1:pqr,h2:jkl,h3:uio	"ABC-123"	{"firstName": "John", "lastName": "Doe", "age": 21}' | kafka-console-producer --bootstrap-server localhost:9092 --topic person --property parse.headers=true --property parse.key=true
```

Produce a message that is invalid for the person schema (the `age` must be greater to equal to 0):

```shell
❯ just person-topic-produce-invalid
echo 'h1:pqr,h2:jkl,h3:uio	"ABC-123"	{"firstName": "John", "lastName": "Doe", "age": -1}' | kafka-console-producer --bootstrap-server localhost:9092 --topic person --property parse.headers=true --property parse.key=true
[2024-12-19 11:51:28,412] ERROR Error when sending message to topic person with key: 19 bytes, value: 51 bytes with error: (org.apache.kafka.clients.producer.internals.ErrorLoggingCallback)
org.apache.kafka.common.InvalidRecordException: This record has failed the validation on broker and hence will be rejected.
```

The server log contains the reason for the message being rejected:

```shell
2024-12-19T11:51:28.407467Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 47: instance=Object {"code": String("ABC-123")}
2024-12-19T11:51:28.407524Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 56: r=()
2024-12-19T11:51:28.407546Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 40: validator=Some(Validator { root: SchemaNode { validators: Keyword, location: Location(""), absolute_path: Some(Uri { scheme: "https", authority: Some(Authority { userinfo: None, host: "example.com", host_parsed: RegName("example.com"), port: None }), path: "/person.schema.json", query: None, fragment: None }) }, config: CompilationConfig { draft: None, content_media_type: [], content_encoding: [] } }) encoded=Some(b"{\"firstName\": \"John\", \"lastName\": \"Doe\", \"age\": -1}")
2024-12-19T11:51:28.407589Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 47: instance=Object {"age": Number(-1), "firstName": String("John"), "lastName": String("Doe")}
2024-12-19T11:51:28.407626Z ERROR peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 51: err=ValidationError { instance: Number(-1), kind: Minimum { limit: Number(0) }, instance_path: Location("/age"), schema_path: Location("/properties/age/minimum") }
2024-12-19T11:51:28.407652Z ERROR peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_schema_registry::json: 57: err=Api(InvalidRecord)
2024-12-19T11:51:28.407724Z ERROR peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: tansu_server::broker::produce: 75: err=Storage(Api(InvalidRecord))
```

[json-schema-org]: https://json-schema.org/
