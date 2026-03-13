## Git Hub Commands
```bash
# Auth to your github acct
gh auth login                               

# List all available releases
gh release list --repo <owwner>/<repo>     #
gh release list --repo DataTalksClub/nyc-tlc-data

# View all assset for a release (tag)
gh release view <tag> --repo <owner>/<repo> 
gh release view green --repo DataTalksClub/nyc-tlc-data

# Download all assets for a relaese
gh release download <tag> --repo <owner>/<repo> 
gh release download green --repo DataTalksClub/nyc-tlc-data

# Add a gitignore template
gh api gitignore/templates/Python --jq '.source' > .gitignore
```

## Base64 Encoding
GCP Creds need to first be base65 encoded then assigned to a variable in a .env file in order for Kestra to use them to create resources in GCP
```bash
base64 -i ~/.config/gcloud/service_accounts/zoomcamp-sa.json | tr -d '\n'
```