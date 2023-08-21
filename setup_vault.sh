VAULT_ADDR="http://vault:8200"
VAULT_TOKEN="token"
file="./secrets.env"

apk --no-cache add curl
apk --no-cache add jq



curl --request POST \
  --header "X-Vault-Token: $VAULT_TOKEN" \
  --data '{"data":{"user":"user","password":"password"}}' \
  "$VAULT_ADDR/v1/secret/data/airflow_secret"


VAULT_DATA=$(curl --request GET --header "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/airflow_secret")


USER=$(echo "$VAULT_DATA" | jq -r '.data.data.user')
PASSWORD=$(echo "$VAULT_DATA" | jq -r '.data.data.password')


echo "_AIRFLOW_WWW_USER_USERNAME=$USER"
echo "_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD"


echo "_AIRFLOW_WWW_USER_USERNAME=$USER" > $file
echo "_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD" >> $file
