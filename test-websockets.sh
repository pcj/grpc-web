#!/bin/bash
set -e

go test -run TestWebSockets ./go/grpcweb
