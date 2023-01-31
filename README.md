# A Basic Object Store for use with FlowForge

## Authorisation

All requests should include a `Authorization` header with a Bearer token assigned by the FlowForge platform to identify
## End Points

### File Storage

- Create/Replace

    **POST** */v1/files/:teamId/:projectId/[path and filename]*

    Content-Type: application/octet-stream

- Append

    **POST** */v1/files/:teamId/:projectId/[path and filename]*

    With Header `FF_MODE: append`

    Content-Type: application/octet-stream
- Read File

    **GET** */v1/files/:teamId/:projectId/[path and filename]*

    Content-Type: application/octet-stream

- Delete File

    **DELETE** */v1/files/:teamId/:projectId/[path and filename]*

- Check team quota usage

    **GET** */v1/quota/:teamId*

    Content-Type: application/json

### Context Store

- Set stored values

    **POST** */v1/context/:projectId/:scope*

    Content-Type: application/json

    Body:
    ```json
    [
        { "key": "x", "value": { "foo": "bar" } },
        { "key": "y.y", "value": 100 },
    ]
    ```

- Get stored values

    **GET** */v1/context/:projectId/:scope?key=x[&key=y.y]*

    Content-Type: application/json

    Response:
    ```json
    [
        { "key": "x", "value": { "foo": "bar" } },
        { "key": "y.y", "value": 100 }
    ]
    ```

- Get keys for a scope

    **GET** */v1/context/:projectId/:scope/keys*

    Content-Type: application/json

    Response:
    ```json
    [
        "x",
        "y"
    ]
    ```

- Delete scope

    **DELETE** */v1/context/:projectId/:scope*

- Clean unused scopes from the store

    **POST** */v1/context/:projectId/clean*

    Content-Type: application/json

    Body:
    ```json
    [
        "nodeId", "flowId"
    ]
    ```

## Configuration

Configuration is read from `etc/flowforge-storage.yml`

```yaml
host: 0.0.0.0
port: 3001
base_url: http://flowforge:3000
driver:
  type: localfs
  options:
    root: var/root
```

- base_url - Where to reach the core FlowForge platform
- driver
    - type - can be `s3`, `localfs` or `memory` (for testing)
    - options - will vary by driver

### File Storage
#### S3

The following can be any of the options for the S3Client Contructor, see [here](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/interfaces/s3clientconfig.html)

- options
    - bucket - name of S3 Bucket (required)
    - region - AWS Region
    - endpoint - S3 ObjectStore Endpoint (if not using AWS S3)
    - forcePathStyle: true/false
    - credential
        - accessKeyId - AccountID/Username
        - secretAccessKey - SecretKey/Password

```yaml
host: '0.0.0.0'
port: 3001
base_url: http://forge.default
driver:
  type: s3
  options:
    bucket: flowforge-files
    credentials:
      accessKeyId: XXXXXXXXXXXXXXXXXXX
      secretAccessKey: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    forcePathStyle: true
    region: us-east-1
```

#### LocalFS

- options
    - root - path to store team files, relative path will apply to FLOWFORGE_HOME

#### Memory

This driver is purely to make testing easier, it has no configuration
options.

### Context Storage

#### Sequelize

This driver can use either PostgreSQL or SQLite to hold context values.

To use with PostgreSQL configure as follows:

```yaml
context:
  type: sequelize
  options:
    type: postgres
    host: 127.0.0.1
    port: 5432
    database: ff-context
    username: user
    password: password
```

To use with SQLite configure as follows:

```yaml
context:
  type: sequelize
  options:
    type: sqlite
    storage: ff-context.db
```

Where `context.options.storage` is the filename of the SQLite database, by default it will be written to 
the `var` directory if a fully qualified path is not provided.

### Environment variables

- FLOWFORGE_HOME default `/opt/flowforge-file-storage`
- PORT overrides value in config file, default 3001

## Development

### Testing

```bash
npm run test
```

NOTE: This will run all tests for all backends and requires a running postgres database

To prepare postgres for the tests, use the following procedure (tested on Linux and WSL2 ubuntu + docker)...

```bash
docker run --rm --name postgres -e POSTGRES_PASSWORD=secret -e POSTGRES_USER=postgres -p 5432:5432 postgres:14
```


### Testing (without postgres)

```
npm run test:nopg
```

Alternatively, you can set env variable TEST_POSTGRES=false
```
export TEST_POSTGRES=false
npm run test
```
