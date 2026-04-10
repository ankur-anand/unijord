BUF ?= buf
PROTO_PATH ?= proto/eventlake/v1

.PHONY: proto proto-build proto-gen proto-lint

proto: proto-build proto-gen

proto-build:
	$(BUF) build

proto-gen: proto-build
	$(BUF) generate --path $(PROTO_PATH)

proto-lint:
	$(BUF) lint --path $(PROTO_PATH)
