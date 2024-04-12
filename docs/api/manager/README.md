# Generating docs from proto files

## Generation

### Setup
```bash
brew install protoc
go install github.com/google/gnostic/cmd/protoc-gen-openapi@latest
```
If needed:
```
export PATH=$PATH:$HOME/go/bin
```

### Generate doc files
```bash
protoc manager_api.proto -I=. --openapi_out=.
```

## Known issues
The generated output has several fields that are not accurate around the error responses and must be manually resolved.

## Resources
* https://github.com/googleapis/googleapis/blob/master/google/api/annotations.proto
* https://github.com/googleapis/googleapis/blob/master/google/api/http.proto
* https://github.com/google/gnostic/issues/412
* https://google.aip.dev/127
