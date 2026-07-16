#!/usr/bin/env bash
set -euo pipefail

readonly secret_name="trade-refresher-api-key"

usage() {
  printf 'Usage: %s <local|sandbox|development|dev|qa|production|prod>\n\n' "$0"
  printf "%s\n" "Generates one API key for manual entry in the selected environment's Azure Key Vault."
  printf 'This script does not connect to Azure or modify Key Vault.\n'
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

requested_environment="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"

case "$requested_environment" in
  local)
    environment="local"
    ;;
  sandbox)
    environment="sandbox"
    ;;
  development|dev)
    environment="development"
    ;;
  qa)
    environment="qa"
    ;;
  production|prod)
    environment="production"
    ;;
  *)
    printf 'Unsupported environment: %s\n' "$1" >&2
    usage >&2
    exit 2
    ;;
esac

if ! command -v openssl >/dev/null 2>&1; then
  printf 'openssl is required to generate the API key.\n' >&2
  exit 1
fi

api_key="$(openssl rand -hex 32)"

printf 'Environment: %s\n' "$environment"
printf 'Azure Key Vault secret name: %s\n' "$secret_name"
printf 'Azure Key Vault secret value: %s\n' "$api_key"
printf 'Treat this value as a secret and do not commit it.\n'
printf 'Create or replace this single secret manually, then allow up to five minutes for the application cache to refresh.\n'

unset api_key
