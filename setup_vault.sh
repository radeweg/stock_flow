VAULT_ADDR="http://vault:8200"
VAULT_TOKEN="token"
file="./secrets.env"
file_postgres="./secrets_postgres.env"

apk --no-cache add curl
apk --no-cache add jq



curl --request POST \
  --header "X-Vault-Token: $VAULT_TOKEN" \
  --data '{"data":{"user":"user","password":"password","postgresuser":"airflow","postgrespassword":"airflow","postgresdb":"airflow"}}' \
  "$VAULT_ADDR/v1/secret/data/airflow_secret"


VAULT_DATA=$(curl --request GET --header "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/airflow_secret")


USER=$(echo "$VAULT_DATA" | jq -r '.data.data.user')
PASSWORD=$(echo "$VAULT_DATA" | jq -r '.data.data.password')
POSTGRES_USER=$(echo "$VAULT_DATA" | jq -r '.data.data.postgresuser')
POSTGRES_PASSWORD=$(echo "$VAULT_DATA" | jq -r '.data.data.postgrespassword')
POSTGRES_DB=$(echo "$VAULT_DATA" | jq -r '.data.data.postgresdb')


echo "_AIRFLOW_WWW_USER_USERNAME=$USER"
echo "_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD"
echo "POSTGRES_USER=$POSTGRES_USER"
echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD"
echo "POSTGRES_DB=$POSTGRES_DB"

echo "_AIRFLOW_WWW_USER_USERNAME=$USER" > $file
echo "_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD" >> $file
echo "POSTGRES_USER=$POSTGRES_USER" > $file_postgres
echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> $file_postgres
echo "POSTGRES_DB=$POSTGRES_DB" >> $file_postgres
