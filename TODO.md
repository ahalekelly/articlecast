# TODO

Make Azure Speech setup easy for anyone deploying their own Articlecast:

- Ship a setup script and link it from the README. It runs `az cognitiveservices account create --kind SpeechServices --sku F0` (and S0) in westus2, then pipes `az cognitiveservices account keys list --query key1 -o tsv` into `gcloud secrets create` and grants the Cloud Run service account access, so setup takes one command with no portal steps or copied keys.
- Replace the Azure Speech keys with keyless auth, also set up by that script: an Entra app registration with a federated credential trusting the Cloud Run service account's Google identity, the "Cognitive Services Speech User" role on both resources, and custom subdomains on them. Articlecast exchanges its Google ID token for an Azure token and refreshes it, so deployers store and rotate no Speech keys.
