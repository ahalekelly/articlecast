# TODO

- Script Azure Speech setup with the Azure CLI: `az cognitiveservices account create --kind SpeechServices --sku F0` (and S0) in westus2, then pipe `az cognitiveservices account keys list --query key1 -o tsv` into `gcloud secrets versions add`, so rebuilding needs no portal steps or copied keys.
- Replace the Azure Speech keys with keyless auth: an Entra app registration with a federated credential trusting the Cloud Run service account's Google identity, the "Cognitive Services Speech User" role on both resources, and custom subdomains on them. Articlecast exchanges its Google ID token for an Azure token and refreshes it, so no Speech keys are stored or rotated.
