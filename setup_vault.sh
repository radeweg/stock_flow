VAULT_ADDR="http://vault:8200"
VAULT_TOKEN="token"
file="./secrets.env"
file_postgres="./secrets_postgres.env"

apk --no-cache add curl
apk --no-cache add jq

curl --request POST \
  --header "X-Vault-Token: $VAULT_TOKEN" \
  --data '{"data":{"user":"user"}}' \
  "$VAULT_ADDR/v1/secret/data/airflow_secret/user"

curl --request POST \
  --header "X-Vault-Token: $VAULT_TOKEN" \
  --data '{"data":{"password":"password"}}' \
  "$VAULT_ADDR/v1/secret/data/airflow_secret/password"


VAULT_USER=$(curl --request GET --header "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/airflow_secret/user")
VAULT_PASSWORD=$(curl --request GET --header "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/airflow_secret/password")


USER=$(echo "$VAULT_USER" | jq -r '.data.data.user')
PASSWORD=$(echo "$VAULT_PASSWORD" | jq -r '.data.data.password')

echo "_AIRFLOW_WWW_USER_USERNAME=$USER" > $file
echo "_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD" >> $file

